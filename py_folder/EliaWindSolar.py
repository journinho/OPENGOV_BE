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
url = "https://opendata.elia.be/explore/dataset/ods031/download/?format=csv&timezone=Europe/Brussels&lang=nl&uselabelsforheader=true&csvseparator=%3B"
fileName = "ods031.csv"
downloadFile(url, fileName, True)
# Read file as dataframe
dfWind = pd.read_csv(f"{sourcePath}{fileName}.gz", sep=";", compression="gzip")

# Download file and store in sourcePath
url = "https://opendata.elia.be/explore/dataset/ods032/download/?format=csv&timezone=Europe/Brussels&lang=nl&uselabelsforheader=true&csvseparator=%3B"
fileName = "ods032.csv"
downloadFile(url, fileName, True)
# Read file as dataframe
dfSolar = pd.read_csv(f"{sourcePath}{fileName}.gz", sep=";", compression="gzip")


# Drop columns we do not use
dfWind.drop(["offshoreonshore", "gridconnectiontype", "mostrecentforecast", "mostrecentconfidence10", "mostrecentconfidence90", "dayahead11hforecast", "dayahead11hconfidence10", "dayahead11hconfidence90", "dayaheadforecast", "dayaheadconfidence10", "dayaheadconfidence90", "weekaheadforecast", "weekaheadconfidence10", "weekaheadconfidence90", "monitoredcapacity", "loadfactor", "decrementalbidid"], axis=1, inplace=True)
dfWind["Type"] = "Wind"
dfSolar.drop(["mostrecentforecast", "dayahead11hforecast", "dayaheadforecast", "weekaheadforecast", "monitoredcapacity", "loadfactor"], axis=1, inplace=True)
dfSolar["Type"] = "Solar"
# Drop rows with no "measured" values (original files also contain predicted values. Those do not have measured values.)
dfWind.dropna(inplace=True)
dfSolar.dropna(inplace=True)

# Add extra date columns
dfWind["datetime"] = pd.to_datetime(dfWind["datetime"], utc=True)
dfWind["Date"] = dfWind["datetime"].dt.date
dfWind["Year"] = dfWind["datetime"].dt.year
dfWind["Month"] = dfWind["datetime"].dt.month
dfWind["YearMonth"] = [datetime.date(i[0], i[1], 15) for i in zip(dfWind["Year"], dfWind["Month"])]

dfSolar["datetime"] = pd.to_datetime(dfSolar["datetime"], utc=True)
dfSolar["Date"] = dfSolar["datetime"].dt.date
dfSolar["Year"] = dfSolar["datetime"].dt.year
dfSolar["Month"] = dfSolar["datetime"].dt.month
dfSolar["YearMonth"] = [datetime.date(i[0], i[1], 15) for i in zip(dfSolar["Year"], dfSolar["Month"])]

dfWindSolar = pd.concat([dfWind, dfSolar])
dfWindSolar

# Daily numbers
dfPivotDateWind = pd.pivot_table(dfWind, values="measured", aggfunc=["mean"], index="Date")
dfPivotDateWind.columns = dfPivotDateWind.columns.get_level_values(1)
# Monthly numbers
dfPivotYearMonthWind = pd.pivot_table(dfWind, values="measured", aggfunc=["mean"], index="YearMonth")
dfPivotYearMonthWind.columns = dfPivotYearMonthWind.columns.get_level_values(1)

# Output to CSV
dfPivotDateWind.to_csv(f"{outputPath}energyProductionWindOverTimeDaily.csv")
dfPivotDateWind.tail(365).to_csv(f"{outputPath}energyProductionWindOverTimeDaily365Days.csv")
dfPivotYearMonthWind.to_csv(f"{outputPath}energyProductionWindOverTimeMonthly.csv")


# Daily numbers
dfPivotDateSolar = pd.pivot_table(dfSolar, values="measured", aggfunc=["mean"], index="Date")
dfPivotDateSolar.columns = dfPivotDateSolar.columns.get_level_values(1)
# Monthly numbers
dfPivotYearMonthSolar = pd.pivot_table(dfSolar, values="measured", aggfunc=["mean"], index="YearMonth")
dfPivotYearMonthSolar.columns = dfPivotYearMonthSolar.columns.get_level_values(1)

# Output to CSV
dfPivotDateSolar.to_csv(f"{outputPath}energyProductionSolarOverTimeDaily.csv")
dfPivotDateSolar.tail(365).to_csv(f"{outputPath}energyProductionSolarOverTimeDaily365Days.csv")
dfPivotYearMonthSolar.to_csv(f"{outputPath}energyProductionSolarOverTimeMonthly.csv")

# Daily numbers
dfPivotDateWindSolar = pd.pivot_table(dfWindSolar, columns=["Type"], values="measured", aggfunc=["mean"], index="Date")
dfPivotDateWindSolar.columns = dfPivotDateWindSolar.columns.get_level_values(1)
# Monthly numbers
dfPivotYearMonthWindSolar = pd.pivot_table(dfWindSolar, columns=["Type"], values="measured", aggfunc=["mean"], index="YearMonth")
dfPivotYearMonthWindSolar.columns = dfPivotYearMonthWindSolar.columns.get_level_values(1)

# Output to CSV
dfPivotDateWindSolar.to_csv(f"{outputPath}energyProductionWindSolarOverTimeDaily.csv")
dfPivotDateWindSolar.tail(365).to_csv(f"{outputPath}energyProductionWindSolarOverTimeDaily365Days.csv")
dfPivotYearMonthWindSolar.to_csv(f"{outputPath}energyProductionWindSolarOverTimeMonthly.csv")
