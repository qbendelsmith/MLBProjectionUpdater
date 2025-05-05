# MLB DFS Projection Updater

A comprehensive Python project for the aggregation, processing, and analyzing of MLB data to create DFS projections with a focus on automating the process of data gathering.

## Overview

This project automates the MLB data collection and analysis from multiple sources, by way of scraping to create projections that that weigh factors I consider crucial to a successful day at the ballpark. It combines traditional statistic, advanced metrics, and the recently introduced real-time Statcast data to offer insights into my approach to fantasy lineup building.

## Features

* Multi-Source Data Scraping: Gathers data from Fangraphs, BaseballSavant, FantasyPros, ESPN, and MLB.com
* Advanced Analytics: Machine Learning pitcher weights, Barrel% analysis, Handedness Splits, Park Factor, Team Offense Stats
* Automated Real-Time Updates: Starting Lineups, Probable Pitchers, DK Salaries, and more to come
* Automated Projections: Generates adjusted projections based on personal preference, with the opportunity to tweak to your liking.
* Excel Integration: Updates to a comprehensive Excel Spreadsheet with VBA Macros.

## Components

1. Python Script (MLBProjectionUpdater.py)

   The automation script:
     * Scrapes from multiple sources
     * Processes and standardizes player names
     * Calculates fantasy projections and averages
     * Updates Excel while preserving the integrity of macros and formulas
     * Generates and gathers advanced metrics for further analysis
       
2. Excel Workbook (MLBProjections.xlsm)

   Macro-enabled workbook with:
     * Player projections and statistics
     * DK Salaries
     * Up to date starting lineups
     * Team and park factors
     * VBA Macros for easier projection and advanced metrics

  3. VBA Macros (VBMacros.bas)

     Custom Excel Macros for:
       * Custom calculations
       * Custom metrics
       * Easy conditional matching

## Prerequisites

- Python 3.8+
- Microsoft Excel with macro support
- Chrome browser (for web scraping)

```bash
pip install pandas openpyxl pybaseball requests selenium webdriver_manager xlwings unicodedata numpy scikit-learn schedule
```

## Installation
1. Clone the repo
```bash
git clone https://github.com/qbendelsmith/MLBProjectionUpdater.git
cd MLBProjectionUpdater
```
2. Place Excel File in Directory
3. Ensure you have an update version of Google Chrome installed for scraping

# How-to run it
## Full Update
```bash
python MLBProjectionUpdater.py --run
```
### Flexible Scheduling Options
The script provides customizable schedule intervals:
```bash
# Run every 6 hours
python MLBProjectionUpdater.py --schedule --hour 6 --minute 0

# Run hourly
python MLBProjectionUpdater.py --schedule --hour 1 --minute 0

# Run every 30 minutes
python MLBProjectionUpdater.py --schedule --hour 0 --minute 30

# Run once daily (default)
python MLBProjectionUpdater.py --schedule

```

### Barrel data only
```bash
python MLBProjectionUpdater.py --barrel-only
```
## Command Line Options

- `--run`: Run the update immediately
- `--schedule`: Schedule daily updates
- `--hour`: Hour to run scheduled update (24-hour format)
- `--minute`: Minute to run scheduled update
- `--barrel-only`: Update only barrel data

## Excel Sheets

The workbook contains the following key sheets:

- **Hitter/Pitcher**: Main projection sheets with calculated adjustments
- **FGHitters/FGPitchers**: FanGraphs season stats
- **FGHittersL7/FGPitchersL30**: Recent performance data
- **FGHittersL3Yrs/FGPitchersL3Yrs**: Three-year averages
- **Salaries**: Current DraftKings salaries
- **TodaysStartingLineups**: Confirmed lineups
- **Statcast**: Barrel rate and batted ball data
- **Parks**: Park factors
- **BatterVsLHP/RHP**: Handedness splits
- **PitcherVsLHB/RHB**: Pitcher handedness splits

  ## Advanced Features

### Pitch Weight Calculation

Uses Ridge regression to calculate a pitch weight metric based on:
- Strikeout rate
- Walk rate
- xFIP
- Hard hit rate
- Ground ball percentage
- Historical DK points

### Adjusted Projections

Calculates adjusted fantasy points considering:
- Expected vs actual HR/FB rates
- Matchup (wOBA, batted ball tendencies)
- Barrel rates
- Park factors
- Opponent strength

### Name Standardization

Name matching system that handles:
- Accents and special characters
- Name variations (Jr., Sr., etc.)
- Multiple name formats
- Player ID matching when available

## Backup System

The script automatically creates timestamped backups before each update to prevent data loss. Backups are stored in the `backups/` directory.

## Logging

Detailed logging is implemented throughout the system:
- Info level: General operation flow
- Warning level: Non-critical issues
- Error level: Critical failures
- Debugging capability
- Log file: `mlb_updater.log`

## Contributing

Contributions are welcome! Please feel free to submit pull requests or create issues for bugs and feature requests.

Always looking to improve the model and dive into new metrics.

## Future Enhancements

Planned improvements include:
- Weather factor (Shoutout to [Kevin Roth](https://x.com/KevinRothWx) at Rotogrinders)
- Umpire factor
- Machine learning model enhancements
- Additional data sources
- Web dashboard interface
- Lineup Optimizing
- Vegas Odds Weights

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This tool is for educational and personal use only. The projections generated are not guaranteed and should be used as part of a broader decision-making process for DFS contests. I am by no means an expert, and simply created this as a small passion project to combine my love of fantasy baseball and coding.

## Author

[Quinn Bendelsmith]

## Acknowledgments
- [pybaseball](https://github.com/jldbc/pybaseball) for FanGraphs data access
- Fangraphs for customizable data and easy access
- Baseball Savant for Statcast data and metrics
- FantasyPros for daily DK salary aggregation
