import pandas as pd
import datetime
import os
import utils

# Metadata for this script
scriptInfo = {
    'scriptID': os.path.splitext(str(os.path.basename(__file__)))[0],
    'scriptPath': str(os.path.abspath(__file__)),
    'sourcePath': 'data/data_clean/energy/',
    'outputPath': 'data/data_bewerkt/energy/',
    'sources': [
        'https://opendata.elia.be/explore/dataset/ods033/download/?format=csv&timezone=Europe/Brussels&lang=nl&use_labels_for_header=true&csv_separator=%3B'
        ],
    'sourcesExplanation': [
        'https://opendata.elia.be/explore/dataset/ods033/information/'
        ]
}
utils.saveScriptInfo(scriptInfo)
# Create input and output folders if they do not exist
utils.createFolder(scriptInfo['sourcePath'])
utils.createFolder(scriptInfo['outputPath'])

# Download file and store in sourcePath
url = scriptInfo['sources'][0]
fileName = "ods033.csv"
utils.downloadFile(url, scriptInfo['sourcePath'], fileName, True)


# Replace Fuel codes with these human readable names
FuelTypes = {
    "CP": "Steenkool",
    "LF": "Olie",
    "NG": "Aardgas",
    "NU": "Nucleair",
    "SO": "Zon",
    "WA": "Water",
    "WI": "Wind",
    "Other": "Ander"
}
# Replace weekday number with these names
weekDagen = ("Maandag", "Dinsdag", "Woensdag", "Donderdag", "Vrijdag", "Zaterdag", "Zondag")

# Read the dataframe
df = pd.read_csv(f"{scriptInfo['sourcePath']}{fileName}.gz", sep=";", compression="gzip")

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
tableFileName = 'energyProductionPerFueltypeOverTime'
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotDateFuel.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")

tableFileName = 'energyProductionPerFueltypeOverTimeAvg7d'
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotDateFuel.rolling(window=7).mean().to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")

# Only use the last 365 days
tableFileName = 'energyProductionPerFueltypeOverTime365Days'
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotDateFuel.tail(365).to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")

tableFileName = 'energyProductionPerFueltypeOverTimeAvg7d365Days'
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotDateFuel.rolling(window=7).mean().tail(365).to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")


# Monthly numbers
dfPivotYearMonthFuel = pd.pivot_table(df, columns=["Fuel code"], values="Generated Power", aggfunc=["mean"], index="YearMonth")
dfPivotYearMonthFuel.columns = dfPivotYearMonthFuel.columns.get_level_values(1)
# Output to CSV
tableFileName = 'energyProductionPerFueltypeOverTimeMonthly'
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotYearMonthFuel.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")


# Weekdays
dfPivotWeekdayFuel = pd.pivot_table(df, columns=["Fuel code"], values="Generated Power", aggfunc=["mean"], index="WeekdayNumber")
dfPivotWeekdayFuel.columns = dfPivotWeekdayFuel.columns.get_level_values(1)
dfPivotWeekdayFuel["Weekdag"] = dfPivotWeekdayFuel.index.map(lambda n: weekDagen[n])
dfPivotWeekdayFuel.set_index('Weekdag', inplace=True)
# Output to CSV
tableFileName = 'energyProductionPerFueltypeWeekdays'
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotWeekdayFuel.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")
