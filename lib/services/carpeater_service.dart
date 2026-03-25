import 'dart:async';
import 'dart:typed_data';
import 'meshcore_protocol.dart';
import 'lora_companion_service.dart';
import 'settings_service.dart';
import 'debug_log_service.dart';

/// Carpeater Mode Service
/// 
/// This service enables "Car Repeater" wardrive mode where instead of using
/// the companion radio directly for discovery, we log into a target repeater
/// and use IT to discover neighbors. This is useful for:
/// - Using a more powerful repeater antenna for discovery
/// - Mapping from a different vantage point (e.g., rooftop repeater)
/// - Getting neighbor data that the repeater has collected
/// 
/// The workflow is:
/// 1. Connect to companion radio (via BLE/USB)
/// 2. Find target repeater in contacts (by public key prefix)
/// 3. Login to the repeater with admin password
/// 4. Loop:
///    a. Request repeater to send an advert (triggers neighbor responses)
///    b. Wait for responses (discovery timeout)
///    c. Fetch neighbor list from repeater
///    d. Process neighbors as discovered nodes
///    e. Optionally clear neighbor list for next cycle
///    f. Wait for next cycle

enum CarpeaterState {
  disabled,
  connecting,
  loggingIn,
  loggedIn,
  discovering,
  fetchingNeighbours,
  error,
}

class CarpeaterService {
  final LoRaCompanionService _loraService;
  final SettingsService _settingsService;
  final _debugLog = DebugLogService();
  final _protocol = MeshCoreProtocol();
  
  // State
  CarpeaterState _state = CarpeaterState.disabled;
  String? _targetRepeaterId;
  String? _targetRepeaterPassword;
  Uint8List? _targetRepeaterPubKeyBytes; // full 32-byte key
  int _discoveryIntervalSeconds = 30;
  bool _isAdmin = false;
  // Completed by stop() to unblock the inter-cycle sleep immediately
  Completer<void>? _stopSignal;

  // Results
  final _neighboursController = StreamController<List<Map<String, dynamic>>>.broadcast();
  final _stateController = StreamController<CarpeaterState>.broadcast();
  /// Fires void once the discovery advert has been confirmed sent to the radio
  /// channel (i.e. RESP_CODE_SENT received).  LocationService listens to this
  /// to snapshot the GPS position at the moment of transmission so that
  /// samples are geo-tagged to WHERE the request was made, not where the phone
  /// is when the results come back 30 s later.
  final _discoveryStartedController = StreamController<void>.broadcast();
  List<Map<String, dynamic>> _lastNeighbours = [];
  DateTime? _lastDiscoveryTime;

  // Response completers — each is non-null only while awaiting that response
  Completer<Map<String, dynamic>?>? _loginCompleter;
  Completer<bool>? _sentCompleter;         // RESP_CODE_SENT after a CLI send
  Completer<Map<String, dynamic>?>? _neighboursCompleter;
  
  CarpeaterService(this._loraService, this._settingsService);
  
  // Public getters
  CarpeaterState get state => _state;
  Stream<CarpeaterState> get stateStream => _stateController.stream;
  Stream<List<Map<String, dynamic>>> get neighboursStream => _neighboursController.stream;
  /// Fires when the discovery advert is confirmed sent — snapshot GPS here.
  Stream<void> get discoveryStartedStream => _discoveryStartedController.stream;
  List<Map<String, dynamic>> get lastNeighbours => List.unmodifiable(_lastNeighbours);
  DateTime? get lastDiscoveryTime => _lastDiscoveryTime;
  bool get isLoggedIn => _state == CarpeaterState.loggedIn || 
                          _state == CarpeaterState.discovering || 
                          _state == CarpeaterState.fetchingNeighbours;
  bool get isAdmin => _isAdmin;
  String? get targetRepeaterId => _targetRepeaterId;
  
  /// Initialize Carpeater mode with settings
  Future<void> initialize() async {
    final enabled = await _settingsService.getCarpeaterEnabled();
    if (!enabled) {
      _setState(CarpeaterState.disabled);
      return;
    }
    
    _targetRepeaterId = await _settingsService.getCarpeaterRepeaterId();
    _targetRepeaterPassword = await _settingsService.getCarpeaterPassword();
    _discoveryIntervalSeconds = await _settingsService.getCarpeaterInterval();
    
    if (_targetRepeaterId == null || _targetRepeaterId!.isEmpty) {
      _debugLog.logError('Carpeater: No target repeater ID configured');
      _setState(CarpeaterState.error);
      return;
    }
    
    if (_targetRepeaterPassword == null || _targetRepeaterPassword!.isEmpty) {
      _debugLog.logError('Carpeater: No password configured');
      _setState(CarpeaterState.error);
      return;
    }
    
    _debugLog.logInfo('Carpeater: Initialized for repeater $_targetRepeaterId');
  }
  
  /// Start Carpeater mode
  /// Requires the LoRa device to be connected first
  Future<bool> start() async {
    if (!_loraService.isDeviceConnected) {
      _debugLog.logError('Carpeater: LoRa device not connected');
      _setState(CarpeaterState.error);
      return false;
    }
    
    await initialize();
    
    if (_state == CarpeaterState.error) {
      return false;
    }
    
    _setState(CarpeaterState.connecting);
    
    // Find the target repeater in contacts
    final found = await _findTargetRepeater();
    if (!found) {
      _debugLog.logError('Carpeater: Target repeater not found in contacts');
      _setState(CarpeaterState.error);
      return false;
    }
    
    // Login to the repeater
    _setState(CarpeaterState.loggingIn);
    final loggedIn = await _loginToRepeater();
    if (!loggedIn) {
      _debugLog.logError('Carpeater: Login failed');
      _setState(CarpeaterState.error);
      return false;
    }
    
    _setState(CarpeaterState.loggedIn);
    _debugLog.logInfo('Carpeater: Logged in successfully (admin=$_isAdmin)');
    
    // Start the discovery loop
    _startDiscoveryLoop();
    
    return true;
  }
  
  /// Stop Carpeater mode
  void stop() {
    // Signal the loop to exit after the current await
    if (_stopSignal != null && !_stopSignal!.isCompleted) {
      _stopSignal!.complete();
    }
    _stopSignal = null;
    // Unblock any in-flight completers so the async stack unwinds cleanly
    _loginCompleter?.complete(null);
    _sentCompleter?.complete(false);
    _neighboursCompleter?.complete(null);
    _loginCompleter = null;
    _sentCompleter = null;
    _neighboursCompleter = null;
    _loraService.setCarpeaterCallback(null);
    _setState(CarpeaterState.disabled);
    _debugLog.logInfo('Carpeater: Stopped');
  }
  
  /// Find the target repeater in the contact list and fetch its pubkey
  Future<bool> _findTargetRepeater() async {
    if (_targetRepeaterId == null) return false;

    _debugLog.logInfo('Carpeater: Looking for repeater $_targetRepeaterId');

    // Try the contact cache that was populated when we connected to the device
    _targetRepeaterPubKeyBytes = _loraService.getContactPubKey(_targetRepeaterId!);
    if (_targetRepeaterPubKeyBytes != null) {
      _debugLog.logInfo('Carpeater: Found pubkey in contact cache');
      return true;
    }

    // Request a fresh contact list and wait for it to arrive
    await _loraService.refreshContactList();
    await Future.delayed(const Duration(seconds: 3));

    _targetRepeaterPubKeyBytes = _loraService.getContactPubKey(_targetRepeaterId!);
    if (_targetRepeaterPubKeyBytes != null) {
      _debugLog.logInfo('Carpeater: Found pubkey after contact refresh');
      return true;
    }

    _debugLog.logError(
      'Carpeater: Repeater $_targetRepeaterId not found in contacts '
      '(make sure it has been seen at least once)',
    );
    return false;
  }
  
  /// Login to the target repeater (retries up to [_maxLoginAttempts] times)
  Future<bool> _loginToRepeater() async {
    if (_targetRepeaterId == null || _targetRepeaterPassword == null) {
      return false;
    }
    if (_targetRepeaterPubKeyBytes == null) {
      _debugLog.logError('Carpeater: Cannot login — no pubkey available');
      return false;
    }

    const maxAttempts = 3;
    const retryDelay = Duration(seconds: 5);

    // Register the callback once for all attempts
    _loraService.setCarpeaterCallback(_handleIncomingPayload);

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      if (attempt > 1) {
        _debugLog.logInfo(
          'Carpeater: Login retry $attempt/$maxAttempts '
          'in ${retryDelay.inSeconds}s...',
        );
        await Future.delayed(retryDelay);
      }

      try {
        _debugLog.logInfo('Carpeater: Sending login command (attempt $attempt/$maxAttempts)...');

        _loginCompleter = Completer<Map<String, dynamic>?>();

        final sent = await _loraService.sendRepeaterLogin(
          targetPubKey: _targetRepeaterPubKeyBytes!,
          password: _targetRepeaterPassword!,
        );
        if (!sent) {
          _loginCompleter = null;
          _debugLog.logError('Carpeater: Failed to enqueue login command');
          continue;
        }

        final response = await _loginCompleter!.future
            .timeout(const Duration(seconds: 10), onTimeout: () => null);
        _loginCompleter = null;

        if (response == null) {
          _debugLog.logError('Carpeater: Login timed out (attempt $attempt)');
          continue; // retry
        }
        if (response['success'] != true) {
          // Explicit rejection from firmware — no point retrying
          _debugLog.logError(
            'Carpeater: Login rejected by repeater (code=${response['error_code']}) — giving up',
          );
          _loraService.setCarpeaterCallback(null);
          return false;
        }

        _isAdmin = response['is_admin'] as bool? ?? false;
        _debugLog.logInfo('Carpeater: Login OK (admin=$_isAdmin)');
        return true;
      } catch (e) {
        _debugLog.logError('Carpeater: Login error (attempt $attempt): $e');
        _loginCompleter = null;
      }
    }

    _loraService.setCarpeaterCallback(null);
    _debugLog.logError('Carpeater: Login failed after $maxAttempts attempts');
    return false;
  }
  
  /// Kick off the sequential discovery loop (fire-and-forget).
  void _startDiscoveryLoop() {
    _stopSignal = Completer<void>();
    _debugLog.logInfo(
      'Carpeater: Discovery loop started (interval: ${_discoveryIntervalSeconds}s)',
    );
    _runDiscoveryLoop();
  }

  /// Runs discovery cycles back-to-back, sleeping [_discoveryIntervalSeconds]
  /// between each one.  Uses [_stopSignal] so stop() wakes the sleep immediately.
  Future<void> _runDiscoveryLoop() async {
    while (_stopSignal != null && !_stopSignal!.isCompleted) {
      await _runDiscoveryCycle();
      if (_stopSignal == null || _stopSignal!.isCompleted) break;
      if (_state == CarpeaterState.error || _state == CarpeaterState.disabled) break;
      _debugLog.logInfo(
        'Carpeater: Cycle complete — waiting ${_discoveryIntervalSeconds}s before next cycle...',
      );
      // Sleep for the configured interval, but wake immediately if stop() fires
      await Future.any([
        Future.delayed(Duration(seconds: _discoveryIntervalSeconds)),
        _stopSignal!.future,
      ]);
    }
    _debugLog.logInfo('Carpeater: Discovery loop exited');
  }
  
  /// Run a single discovery cycle
  Future<void> _runDiscoveryCycle() async {
    if (_state == CarpeaterState.disabled || _state == CarpeaterState.error) {
      return;
    }
    
    try {
      _setState(CarpeaterState.discovering);
      _debugLog.logInfo('Carpeater: Starting discovery cycle...');
      
      // Step 1: Clear the repeater's neighbour table so this cycle starts fresh.
      // Uses `neighbor.remove ` (empty hex arg → key_len=0 → clears all entries).
      // Waits for RESP_CODE_SENT before proceeding.  Logs a warning but continues
      // if the clear fails — stale neighbours are better than skipping discovery.
      final clearOk = await _clearPreviousNeighbours();
      if (!clearOk) {
        _debugLog.logError('Carpeater: Could not clear neighbours — continuing anyway');
      }

      // Step 2: Tell the repeater to broadcast a CTL_TYPE_NODE_DISCOVER_REQ
      // (sends the "discover.neighbors" CLI command to the repeater).
      // Waits for RESP_CODE_SENT before starting the 30 s wait.
      final advertOk = await _triggerRepeaterAdvert();
      if (!advertOk) {
        _debugLog.logError('Carpeater: Could not trigger repeater advert — skipping this cycle');
        _setState(CarpeaterState.loggedIn);
        return;
      }

      // Notify listeners (LocationService) to snapshot GPS position NOW — this
      // is where the discovery was physically sent, not where we'll be in 30 s.
      _discoveryStartedController.add(null);
      
      // Step 3: Wait 30 s for nearby nodes to hear the request and respond.
      // The repeater listens for up to ~60 s, but 30 s gives good coverage
      // while keeping the wardrive cycle reasonable.
      const discoveryWaitSeconds = 30;
      _debugLog.logInfo('Carpeater: Waiting ${discoveryWaitSeconds}s for responses...');
      await Future.delayed(const Duration(seconds: discoveryWaitSeconds));
      
      // Step 4: Fetch the neighbours list (with retries)
      _setState(CarpeaterState.fetchingNeighbours);
      final neighbours = await _fetchNeighbours();
      
      if (neighbours != null && neighbours.isNotEmpty) {
        _lastNeighbours = neighbours;
        _lastDiscoveryTime = DateTime.now();
        _neighboursController.add(neighbours);
        _debugLog.logInfo('Carpeater: Found ${neighbours.length} neighbours');
        for (final n in neighbours) {
          _debugLog.logInfo(
            '  - ${n['pubkey']}: SNR=${n['snr']}, heard ${n['heard_seconds_ago']}s ago',
          );
        }
      } else {
        _debugLog.logInfo('Carpeater: No neighbours found this cycle');
        // Emit an empty list so LocationService can record a failed sample
        // (dead zone — repeater heard nobody).
        _neighboursController.add([]);
      }
      
      _setState(CarpeaterState.loggedIn);
      
    } catch (e) {
      _debugLog.logError('Carpeater: Discovery cycle error: $e');
      _setState(CarpeaterState.error);
    }
  }
  
  /// Tell the repeater to broadcast CTL_TYPE_NODE_DISCOVER_REQ.
  /// Waits for RESP_CODE_SENT from the companion radio confirming the packet
  /// reached the radio channel before returning.  Retries 3 times.
  Future<bool> _triggerRepeaterAdvert() async {
    if (_targetRepeaterPubKeyBytes == null) {
      _debugLog.logError('Carpeater: Cannot trigger discovery — no pubkey');
      return false;
    }

    const maxAttempts = 3;
    const retryDelay = Duration(seconds: 3);

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      if (attempt > 1) {
        _debugLog.logInfo(
          'Carpeater: Discovery trigger retry $attempt/$maxAttempts '
          'in ${retryDelay.inSeconds}s...',
        );
        await Future.delayed(retryDelay);
      }
      try {
        _debugLog.logInfo(
          'Carpeater: Sending discover.neighbors (attempt $attempt/$maxAttempts)...',
        );
        _sentCompleter = Completer<bool>();
        final enqueued = await _loraService.sendRepeaterCliCommand(
          targetPubKey: _targetRepeaterPubKeyBytes!,
          command: 'discover.neighbors',
        );
        if (!enqueued) {
          _sentCompleter = null;
          _debugLog.logError('Carpeater: Failed to enqueue discover command');
          continue;
        }
        final acked = await _sentCompleter!.future
            .timeout(const Duration(seconds: 10), onTimeout: () => false);
        _sentCompleter = null;
        if (acked) {
          _debugLog.logInfo('Carpeater: discover.neighbors sent (ACK received)');
          return true;
        }
        _debugLog.logError(
          'Carpeater: discover.neighbors not acknowledged (attempt $attempt)',
        );
      } catch (e) {
        _debugLog.logError(
          'Carpeater: Discovery trigger error (attempt $attempt): $e',
        );
        _sentCompleter = null;
      }
    }
    _debugLog.logError(
      'Carpeater: Discovery trigger failed after $maxAttempts attempts',
    );
    return false;
  }
  
  /// Fetch the neighbours list from the repeater (retries up to 3 times)
  Future<List<Map<String, dynamic>>?> _fetchNeighbours() async {
    if (_targetRepeaterPubKeyBytes == null) {
      _debugLog.logError('Carpeater: Cannot fetch neighbours — no pubkey');
      return null;
    }

    const maxAttempts = 3;
    const retryDelay = Duration(seconds: 5);

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      if (attempt > 1) {
        _debugLog.logInfo(
          'Carpeater: Neighbour fetch retry $attempt/$maxAttempts '
          'in ${retryDelay.inSeconds}s...',
        );
        await Future.delayed(retryDelay);
      }

      try {
        _debugLog.logInfo(
          'Carpeater: Requesting neighbour list (attempt $attempt/$maxAttempts)...',
        );

        _neighboursCompleter = Completer<Map<String, dynamic>?>();

        final sent = await _loraService.sendRepeaterGetNeighbours(
          targetPubKey: _targetRepeaterPubKeyBytes!,
        );
        if (!sent) {
          _neighboursCompleter = null;
          _debugLog.logError('Carpeater: Failed to enqueue neighbour request');
          continue;
        }

        final response = await _neighboursCompleter!.future
            .timeout(const Duration(seconds: 15), onTimeout: () => null);
        _neighboursCompleter = null;

        if (response == null) {
          _debugLog.logError('Carpeater: Neighbour fetch timed out (attempt $attempt)');
          continue; // retry
        }

        final neighbours =
            (response['neighbours'] as List?)?.cast<Map<String, dynamic>>() ?? [];
        _debugLog.logInfo(
          'Carpeater: Received ${neighbours.length} of ${response['total_count']} neighbours',
        );
        return neighbours;
      } catch (e) {
        _debugLog.logError('Carpeater: Neighbour fetch error (attempt $attempt): $e');
        _neighboursCompleter = null;
      }
    }

    _debugLog.logError('Carpeater: Neighbour fetch failed after $maxAttempts attempts');
    return null;
  }
  
  /// Wipe the repeater's neighbour table.
  ///
  /// Sends `"neighbor.remove "` (empty hex arg → key_len=0 → all entries match
  /// in `removeNeighbor()`) then waits for RESP_CODE_SENT confirming the
  /// companion radio pushed the packet to the radio channel.  Retries 3 times.
  Future<bool> _clearPreviousNeighbours() async {
    if (_targetRepeaterPubKeyBytes == null) return false;

    const maxAttempts = 3;
    const retryDelay = Duration(seconds: 3);

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      if (attempt > 1) {
        _debugLog.logInfo(
          'Carpeater: Clear retry $attempt/$maxAttempts in ${retryDelay.inSeconds}s...',
        );
        await Future.delayed(retryDelay);
      }
      try {
        _debugLog.logInfo(
          'Carpeater: Clearing neighbour table (attempt $attempt/$maxAttempts)...',
        );
        _sentCompleter = Completer<bool>();
        final enqueued = await _loraService.sendRepeaterCliCommand(
          targetPubKey: _targetRepeaterPubKeyBytes!,
          command: 'neighbor.remove ',
        );
        if (!enqueued) {
          _sentCompleter = null;
          _debugLog.logError('Carpeater: Failed to enqueue clear command');
          continue;
        }
        final acked = await _sentCompleter!.future
            .timeout(const Duration(seconds: 10), onTimeout: () => false);
        _sentCompleter = null;
        if (acked) {
          _debugLog.logInfo('Carpeater: Neighbour table cleared (sent ACK received)');
          return true;
        }
        _debugLog.logError(
          'Carpeater: Clear not acknowledged by companion radio (attempt $attempt)',
        );
      } catch (e) {
        _debugLog.logError('Carpeater: Clear error (attempt $attempt): $e');
        _sentCompleter = null;
      }
    }
    _debugLog.logError('Carpeater: Failed to clear neighbour table after $maxAttempts attempts');
    return false;
  }

  /// Route incoming push frames to the right completer.
  /// Registered with LoRaCompanionService via setCarpeaterCallback.
  void _handleIncomingPayload(int pushCode, Uint8List data) {
    switch (pushCode) {
      case PUSH_CODE_LOGIN_SUCCESS:
        if (_loginCompleter != null && !_loginCompleter!.isCompleted) {
          final result = _protocol.parseLoginSuccessPush(data);
          _debugLog.logInfo('Carpeater: Received login SUCCESS');
          _loginCompleter!.complete(result);
        }
        break;

      case PUSH_CODE_LOGIN_FAIL:
        if (_loginCompleter != null && !_loginCompleter!.isCompleted) {
          final result = _protocol.parseLoginFailPush(data);
          _debugLog.logError('Carpeater: Received login FAIL');
          _loginCompleter!.complete(result);
        }
        break;

      case RESP_CODE_SENT:
        // Companion radio confirmed the CLI packet was sent to the radio channel
        if (_sentCompleter != null && !_sentCompleter!.isCompleted) {
          _debugLog.logInfo('Carpeater: CLI send confirmed (RESP_CODE_SENT)');
          _sentCompleter!.complete(true);
        }
        break;

      case PUSH_CODE_BINARY_RESPONSE:
        if (_neighboursCompleter != null && !_neighboursCompleter!.isCompleted) {
          final result = _protocol.parseBinaryResponseNeighbours(data, 8);
          if (result != null) {
            _debugLog.logInfo('Carpeater: Received neighbours response via 0x8C');
            _neighboursCompleter!.complete(result);
          } else {
            _debugLog.logLoRa('Carpeater: Binary response could not be parsed as neighbours');
          }
        }
        break;

      default:
        _debugLog.logLoRa(
          'Carpeater: Unhandled push 0x${pushCode.toRadixString(16)} '
          '(${data.length} bytes)',
        );
    }
  }
  
  /// Set state and notify listeners
  void _setState(CarpeaterState newState) {
    if (_state != newState) {
      _state = newState;
      _stateController.add(newState);
      _debugLog.logInfo('Carpeater: State changed to ${newState.name}');
    }
  }
  
  void dispose() {
    stop();
    _neighboursController.close();
    _stateController.close();
  }
}
