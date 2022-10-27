import pandas as pd
import datetime
import os
import sys
import requests
import gzip


outputPath = "data/data_bewerkt/energy/"
sourcePath = "data/data_clean/energy/"
# Create folder if it does not exist
if not os.path.exists(outputPath):
    print("Making directory", outputPath)
    os.makedirs(outputPath)
if not os.path.exists(sourcePath):
    print("Making directory", sourcePath)
    os.makedirs(sourcePath)


def downloadFile(url: str, fileName: str, asZipped: bool = True):
    """Download a file from url and save it, optionally as a gzipped file.

    :param url: url to download the file from
    :param fileName: filename to give the downloaded file
    :param asZipped: save it in gzipped form
    """
    try:
        r = requests.get(url, allow_redirects=True)
    except Exception as e:
        print(f"Downloading {fileName} failed: ", e)
        sys.exit(1)
    try:
        if asZipped:
            with gzip.open(f"{sourcePath}{fileName}.gz", 'wb') as f:
                f.write(r.content)
        else:
            with open(f"{sourcePath}{fileName}", 'wb') as f:
                f.write(r.content)
    except Exception as e:
        print(f"Saving file {fileName} failed: ", e)
        sys.exit(1)


# Download file and store in sourcePath
url = "https://opendata.elia.be/explore/dataset/ods033/download/?format=csv&timezone=Europe/Brussels&lang=nl&use_labels_for_header=true&csv_separator=%3B"
fileName = "ods033.csv"
downloadFile(url, fileName, True)


# Replace Fuel codes with these human readable names
FuelTypes = {
    "CP": "Steenkool",
    "LF": "Olie",
    "NG": "Aardgas",
    "NU": "Nucleair",
    "SO": "Zon",
    "WA": "Water",
    "WI": "Wind",
    "OTHER": "Ander"
}
# Replace weekday number with these names
weekDagen = ("Maandag", "Dinsdag", "Woensdag", "Donderdag", "Vrijdag", "Zaterdag", "Zondag")

# Read the dataframe
df = pd.read_csv(f"{sourcePath}{fileName}.gz", sep=";", compression="gzip")


# Replace Fuel codes with Fuel Names
df.replace({"Fuel code": FuelTypes}, inplace=True)

# Add extra date columns
df["Datetime"] = pd.to_datetime(df["Datetime"], utc=True)
df["Date"] = df["Datetime"].dt.date
df["Year"] = df["Datetime"].dt.year
df["Month"] = df["Datetime"].dt.month
df["YearMonth"] = [datetime.date(i[0], i[1], 15) for i in zip(df["Year"], df["Month"])]
df["WeekdayNumber"] = df["Datetime"].dt.weekday
df["WeekdayName"] = df["Datetime"].dt.weekday.map(lambda n: weekDagen[n])


# Daily numbers
dfPivotDateFuel = pd.pivot_table(df, columns=["Fuel code"], values="Generated Power", aggfunc=["mean"], index="Date")
dfPivotDateFuel.columns = dfPivotDateFuel.columns.get_level_values(1)
# Output to CSV
dfPivotDateFuel.to_csv(f"{outputPath}energyProductionPerFueltypeOverTime.csv")
dfPivotDateFuel.rolling(window=7).mean().to_csv(f"{outputPath}energyProductionPerFueltypeOverTimeAvg7d.csv")
# Only use the last 365 days
dfPivotDateFuel.tail(365).to_csv(f"{outputPath}energyProductionPerFueltypeOverTime365Days.csv")
dfPivotDateFuel.rolling(window=7).mean().tail(365).to_csv(f"{outputPath}energyProductionPerFueltypeOverTimeAvg7d365Days.csv")


# Monthly numbers
dfPivotYearMonthFuel = pd.pivot_table(df, columns=["Fuel code"], values="Generated Power", aggfunc=["mean"], index="YearMonth")
dfPivotYearMonthFuel.columns = dfPivotYearMonthFuel.columns.get_level_values(1)
# Output to CSV
dfPivotYearMonthFuel.to_csv(f"{outputPath}energyProductionPerFueltypeOverTimeMonthly.csv")


# Weekdays
dfPivotWeekdayFuel = pd.pivot_table(df, columns=["Fuel code"], values="Generated Power", aggfunc=["mean"], index="WeekdayNumber")
dfPivotWeekdayFuel.columns = dfPivotWeekdayFuel.columns.get_level_values(1)
dfPivotWeekdayFuel["WeekdayName"] = dfPivotWeekdayFuel.index.map(lambda n: weekDagen[n])
dfPivotWeekdayFuel.set_index('WeekdayName', inplace=True)
# Output to CSV
dfPivotWeekdayFuel.to_csv(f"{outputPath}energyProductionPerFueltypeWeekdays.csv")
