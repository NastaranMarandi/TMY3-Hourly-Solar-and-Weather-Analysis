```markdown
# TMY3 Hourly Solar and Weather Analysis

A Python script to process TMY3 solar and weather data, aggregate it by week, and export the results in both CSV and JSON formats.

## Prerequisites

- Python 3.7+
- pandas library

Install dependencies:
```bash
pip install pandas
```

## Setup Instructions

### 1. Download the Data

Download the TMY3 weather and station data from:
```
https://www.kaggle.com/datasets/us-doe/tmy3-solar/data
```

### 2. Prepare Input Data

1. Extract the downloaded files
2. Place the following files in the input_data folder:
   - `tmy3.csv` - Hourly weather observations (GHI, DNI, and other weather variables)
   - `TMY3_StationsMeta.csv` - Station metadata (names, coordinates, etc.)

Your folder structure should look like:
```
.
├── TMY3_solar_process.py
├── input_data/
│   ├── tmy3.csv
│   └── TMY3_StationsMeta.csv
└── output/
    ├── weekly_weather.csv
    └── weekly_weather.json
```

## Running the Script

Execute the script from the command line:

```bash
python TMY3_solar_process.py
```

## Output Files

After running the script, JSON output file will be generated in the output folder.

## Data Processing Steps

1. **Load**: Reads CSV files with proper data types
2. **Explore**: Displays data quality information
3. **Clean**: 
   - Removes duplicates
   - Converts dates/times to datetime format
   - Converts irradiance values to numeric format
   - Handles missing values by filling with station-specific means
   - Merges station metadata with weather observations
4. **Calculate**: Aggregates hourly data to weekly averages
5. **Save**: Exports results to JSON and CSV formats
