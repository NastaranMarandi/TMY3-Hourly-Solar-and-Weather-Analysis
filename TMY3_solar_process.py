# TMY3 Solar Processor
import json
from pathlib import Path
import pandas as pd


# Base directory
BASE_DIR = Path(__file__).resolve().parent

# Files
WEATHER_FILE = "tmy3.csv"
STATIONS_META_FILE = "TMY3_StationsMeta.csv"

DATA_DIR = BASE_DIR / "input_data"

# Output directory
OUTPUT_DIR = BASE_DIR / "output"


def load_data():
    """Load stations metadata and weather data from CSV files."""
    stations_meta_path = DATA_DIR / STATIONS_META_FILE
    weather_path = DATA_DIR / WEATHER_FILE

    stations_meta = pd.read_csv(
        stations_meta_path, header=0, dtype={"USAF": str})

    weather = pd.read_csv(weather_path, header=0, dtype=str)

    if stations_meta.empty:
        raise ValueError("Stations metadata is empty!")

    if weather.empty:
        raise ValueError("Weather data is empty!")

    print(f"Stations Meta data shape: {stations_meta.shape}")
    print(f"Weather data shape: {weather.shape}")

    return stations_meta, weather


def explore(stations_meta, weather):
    """Display summary information about the loaded data."""
    print("=== Stations Meta Data ===")
    print(stations_meta.head(3).to_string())
    print(f"Nulls:\n{stations_meta.isnull().sum()[stations_meta.isnull().sum() > 0]}\n")

    print("=== WEATHER ===")
    print(weather[["Date (MM/DD/YYYY)", "Time (HH:MM)", "GHI (W/m^2)",
          "DNI (W/m^2)", "station_number"]].head(3).to_string())
    print(f"Nulls in GHI: {weather['GHI (W/m^2)'].isnull().sum()}, DNI: {weather['DNI (W/m^2)'].isnull().sum()}\n")


def clean(stations_meta, weather):
    """Clean and preprocess stations metadata and weather data."""
    # Stations Meta
    stations_meta["USAF"] = stations_meta["USAF"].str.strip()
    stations_meta["Latitude"] = pd.to_numeric(
        stations_meta["Latitude"],  errors="coerce")
    stations_meta["Longitude"] = pd.to_numeric(
        stations_meta["Longitude"], errors="coerce")
    # Check for duplicate USAF stations
    duplicates = stations_meta.duplicated(subset=["USAF"]).sum()
    if duplicates > 0:
        print(f"Duplicate USAF stations: {duplicates}")
        stations_meta = stations_meta.drop_duplicates(subset=["USAF"], keep="first")
        print(f"Dropped duplicates, new shape: {stations_meta.shape}")
    else:
        print(f"No duplicate USAF stations rows.")
    
    # Weather
    weather["datetime"] = pd.to_datetime(
        weather["Date (MM/DD/YYYY)"].str.strip() + " " +
        weather["Time (HH:MM)"].str.strip(),
        format="%m/%d/%Y %H:%M", errors="coerce"
    )
    weather["GHI"] = pd.to_numeric(weather["GHI (W/m^2)"], errors="coerce")
    weather["DNI"] = pd.to_numeric(weather["DNI (W/m^2)"], errors="coerce")
    weather["USAF"] = weather["station_number"].str.strip()
    weather = weather[["USAF", "datetime", "GHI", "DNI"]].dropna(subset=["datetime"])
    # Fill null GHI and DNI with each station's mean rather than dropping rows
    weather["GHI"] = weather.groupby("USAF")["GHI"].transform(lambda x: x.fillna(x.mean()))
    weather["DNI"] = weather.groupby("USAF")["DNI"].transform(lambda x: x.fillna(x.mean()))
    print(f"GHI nulls after imputations: {weather['GHI'].isnull().sum()}, DNI nulls: {weather['DNI'].isnull().sum()}")
    # Check for duplicate (USAF, datetime) pairs
    duplicates = weather.duplicated(subset=["USAF", "datetime"]).sum()
    if duplicates > 0:
        print(f"Duplicate (USAF, datetime) rows: {duplicates}")
        weather = weather.drop_duplicates(subset=["USAF", "datetime"], keep="first")
        print(f"Dropped duplicates, new shape: {weather.shape}")
    else:
        print(f"No duplicate (USAF, datetime) rows.")

    # Join station name and coordinates from stations meta data to weather data
    weather = weather.merge(
        stations_meta[["USAF", "Site Name", "Latitude", "Longitude"]], on="USAF", how="left")
    print(f"Merged Data: {weather.head(3)}")
    return weather


def calculate(weather):
    """Calculate weekly aggregated statistics from weather data."""
    weather["week_start"] = weather["datetime"].dt.to_period("W-MON").dt.start_time

    weekly = (
        weather.groupby(["USAF", "Site Name", "Latitude", "Longitude", "week_start"])[["GHI", "DNI"]]
        .mean()
        .reset_index()
        .rename(columns={"week_start": "datetime"})
    )
    weekly["timestamp_ms"] = weekly["datetime"].view("int64") // 1000000
    return weekly

def save(weekly: pd.DataFrame) -> None:
    """Save processed weather data to CSV and JSON files."""
    # Save as CSV
    weekly.to_csv(OUTPUT_DIR / "weekly_weather.csv", index=False)
    print("Data saved as CSV file in the ouput folder.")
    # Build JSON structure
    stations = []
    for usaf, group in weekly.groupby("USAF"):
        stations.append({
            "id": usaf,
            "site_name": group.iloc[0]["Site Name"],
            "coordinates": [float(group.iloc[0]["Longitude"]), float(group.iloc[0]["Latitude"])],
            "data": [
                {"timestamp": int(row.timestamp_ms),
                 "ghi": round(float(row.GHI), 4) if pd.notna(row.GHI) else None,
                 "dni": round(float(row.DNI), 4) if pd.notna(row.DNI) else None}
                for row in group.itertuples()
            ]
        })

    # Save as JSON
    with open(OUTPUT_DIR / "weekly_weather.json", "w", encoding="utf-8") as f:
        json.dump(stations, f, indent=2)
    print("Data saved as JSON file in the ouput folder.")


def main():
    """Execute the TMY3 solar and weather data processing pipeline."""
    stations_meta, weather = load_data()
    explore(stations_meta, weather)
    weather = clean(stations_meta, weather)
    weekly = calculate(weather)
    save(weekly)


if __name__ == "__main__":
    main()
