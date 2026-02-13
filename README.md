# MeshCore Wardrive

A Flutter-based Android application for mapping MeshCore mesh network coverage in real-time.

## 📥 Downloads

**Latest Release APK:** Available in the [releases](releases/) folder

**Source Code:** [Meshcore-Wardrive-Android-Source](https://github.com/mintylinux/Meshcore-Wardrive-Android-Source)

## 🚀 Quick Start

1. **Download the APK** from the releases folder
2. **Enable "Install from Unknown Sources"** in your Android settings
3. **Install the APK** on your device
4. **Connect your LoRa device** via USB or Bluetooth
5. **Start wardriving!**

## ✨ Features

### Mobile App
- **Real-time GPS Tracking** - Background support with screen-lock capability
- **LoRa Device Support** - Connect via USB or Bluetooth to MeshCore companion radios
- **Auto-Ping Mode** - Configurable intervals from 50 meters to 1 mile
- **Manual Ping** - Test coverage at specific locations
- **Success Rate Visualization** - Color-coded coverage map:
  - 🟢 **Green (≥80%)**: Very reliable coverage
  - 🟢 **Yellow-Green (50-80%)**: Usually works
  - 🟡 **Yellow (30-50%)**: Spotty coverage
  - 🟠 **Orange (10-30%)**: Rarely works
  - 🔴 **Red (<10%)**: Dead zone
- **Repeater Discovery** - See which repeaters are in range
- **Data Export** - Save and share your wardrive data
- **Web Map Upload** - Contribute to community coverage map

### Web Map
View the live community coverage map at: **https://meshwar-map.pages.dev**

- Interactive coverage visualization
- Success rate analytics per grid square
- Repeater tracking
- Real-time updates every 30 seconds

## 📋 Requirements

- Android device with GPS
- MeshCore companion radio device

## 🎯 How It Works

1. The app connects to your MeshCore companion radio
3. Repeaters that hear your ping respond with an echo
4. The app records which repeaters responded and their signal quality
5. Coverage is visualized as colored grid squares (~1.2km x 610m)
6. Data can be uploaded to the web map for community viewing

## 🔧 Configuration

### Adjustable Settings
- **Ping Interval**: 50m, 200m, 0.5 miles, or 1 mile
- **Ignore Mobile Repeater**: Filter out your own mobile repeater by prefix
- **Map Theme**: Light or dark mode
- **Color Mode**: Quality (signal strength) or Age (data freshness)

### Default Settings
- Ping interval: 0.5 miles (805 meters)
- Grid size: ~1.2km x 610m squares (precision 6)
- GPS accuracy: High
- Update frequency: Every 5 meters of movement

## 🎨 Understanding the Map

**Grid Squares** show coverage quality based on success rate:
- Multiple users' pings are aggregated per square
- Color shows: `successful pings / total pings`
- More samples = more accurate representation
- Gray squares = no ping data (GPS tracking only)

**Each square popup shows:**
- Number of samples collected
- Success rate percentage
- Received vs. lost pings
- Number of repeaters heard
- Repeater IDs (first 2 characters)

## 📖 Usage Tips

1. **Connect Device**: Use USB for most reliable connection
2. **Enable Tracking**: Press green play button to start GPS tracking
3. **Enable Auto-Ping**: Toggle the switch to auto-ping while driving
4. **Manual Testing**: Use manual ping button to test specific locations
5. **Upload Data**: Share your findings via Settings → Upload Data

## 🐛 Troubleshooting

**Location not updating?**
- Check GPS permissions are granted
- Enable "Location Always" for background tracking
- Press the manual location button to refresh

**Can't connect to device?**
- Check USB cable supports data transfer (not just charging)
- For Bluetooth: Make sure device is paired in Android settings first
- Try disconnecting and reconnecting 
  
**No repeater responses?**
- Verify you're in an area with repeater coverage
- Check LoRa device is connected (icon in app)
- Try manual ping to test connection

## 📊 Statistics

The top panel shows:
- **Total Samples**: Number of pings collected
- **Total Repeaters Heard**: Unique repeaters across entire map
- **Last Update**: When data was last refreshed

## 🔐 Privacy

- GPS data is only stored locally on your device
- Only uploaded to web map when you explicitly choose to upload
- No personal information is collected or transmitted
- Repeater IDs are shortened to first 2 characters on web map

## 🤝 Contributing to Coverage

Help build the community coverage map:
1. Wardrive your local area
2. Open Settings → Upload Data
3. Your samples contribute to the global success rate statistics

The more users contribute, the more accurate the coverage map becomes!

## 🆘 Support

For issues, questions, or feature requests:
- Open an issue on GitHub
- Check existing issues for solutions

## 🛠️ Development

For developers who want to build from source, see the [source code repository](https://github.com/mintylinux/Meshcore-Wardrive-Android-Source) which includes:

- Complete Flutter project structure
- Development setup instructions
- Build configuration
- Architecture documentation
- Contributing guidelines

## 📄 License

[Specify your license here]

## 🙏 Credits

Built for the MeshCore mesh networking community.

## 📧 Support

For issues and questions:
- Open an issue on GitHub
- Check existing documentation in the repository

---

**Current Version:** 1.0.24

**Minimum Android Version:** Android 5.0 (API 21)

**Happy Wardriving! 🚗📡**
