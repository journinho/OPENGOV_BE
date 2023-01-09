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
        'https://opendata.elia.be/explore/dataset/ods031/download/?format=csv&timezone=Europe/Brussels&lang=nl&uselabelsforheader=true&csvseparator=%3B',
        'https://opendata.elia.be/explore/dataset/ods032/download/?format=csv&timezone=Europe/Brussels&lang=nl&uselabelsforheader=true&csvseparator=%3B'
        ],
    'sourcesExplanation': [
        'https://opendata.elia.be/explore/dataset/ods031/information/',
        'https://opendata.elia.be/explore/dataset/ods032/information/'
        ]
}
utils.saveScriptInfo(scriptInfo)
# Create input and output folders if they do not exist
utils.createFolder(scriptInfo['sourcePath'])
utils.createFolder(scriptInfo['outputPath'])


# Download file and store in sourcePath
url = scriptInfo['sources'][0]
fileName = "ods031.csv"
utils.downloadFile(url, scriptInfo['sourcePath'], fileName, True)

# Read file as dataframe
dfWind = pd.read_csv(f"{scriptInfo['sourcePath']}{fileName}.gz", sep=";", compression="gzip")
dfWind.rename(columns={'measured': 'MW'}, inplace = True)


# Download file and store in sourcePath
url = scriptInfo['sources'][1]
fileName = "ods032.csv"
utils.downloadFile(url, scriptInfo['sourcePath'], fileName, True)

# Read file as dataframe
dfSolar = pd.read_csv(f"{scriptInfo['sourcePath']}{fileName}.gz", sep=";", compression="gzip")
dfSolar.rename(columns={'measured': 'MW'}, inplace = True)


# Drop columns we do not use
dfWind.drop(["offshoreonshore", "gridconnectiontype", "mostrecentforecast", "mostrecentconfidence10", "mostrecentconfidence90", "dayahead11hforecast", "dayahead11hconfidence10", "dayahead11hconfidence90", "dayaheadforecast", "dayaheadconfidence10", "dayaheadconfidence90", "weekaheadforecast", "weekaheadconfidence10", "weekaheadconfidence90", "monitoredcapacity", "loadfactor", "decrementalbidid"], axis=1, inplace=True)
dfWind["Type"] = "Wind"
dfSolar.drop(["mostrecentforecast", "dayahead11hforecast", "dayaheadforecast", "weekaheadforecast", "monitoredcapacity", "loadfactor"], axis=1, inplace=True)
dfSolar["Type"] = "Zon"
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

# Daily numbers
dfPivotDateWind = pd.pivot_table(dfWind, values="MW", aggfunc=["mean"], index="Date")
dfPivotDateWind.columns = dfPivotDateWind.columns.get_level_values(1)
# Monthly numbers
dfPivotYearMonthWind = pd.pivot_table(dfWind, values="MW", aggfunc=["mean"], index="YearMonth")
dfPivotYearMonthWind.columns = dfPivotYearMonthWind.columns.get_level_values(1)

# Output to CSV
tableFileName = "energyProductionWindOverTimeDaily"
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotDateWind.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")

tableFileName = "energyProductionWindOverTimeDaily365Days"
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotDateWind.tail(365).to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")

tableFileName = "energyProductionWindOverTimeMonthly"
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotYearMonthWind.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")


# Daily numbers
dfPivotDateSolar = pd.pivot_table(dfSolar, values="MW", aggfunc=["mean"], index="Date")
dfPivotDateSolar.columns = dfPivotDateSolar.columns.get_level_values(1)
# Monthly numbers
dfPivotYearMonthSolar = pd.pivot_table(dfSolar, values="MW", aggfunc=["mean"], index="YearMonth")
dfPivotYearMonthSolar.columns = dfPivotYearMonthSolar.columns.get_level_values(1)

# Output to CSV
tableFileName = "energyProductionSolarOverTimeDaily"
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotDateSolar.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")

tableFileName = "energyProductionSolarOverTimeDaily365Days"
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotDateSolar.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")

tableFileName = "energyProductionSolarOverTimeMonthly"
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotYearMonthSolar.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")


# Daily numbers
dfPivotDateWindSolar = pd.pivot_table(dfWindSolar, columns=["Type"], values="MW", aggfunc=["mean"], index="Date")
dfPivotDateWindSolar.columns = dfPivotDateWindSolar.columns.get_level_values(1)
# Monthly numbers
dfPivotYearMonthWindSolar = pd.pivot_table(dfWindSolar, columns=["Type"], values="MW", aggfunc=["mean"], index="YearMonth")
dfPivotYearMonthWindSolar.columns = dfPivotYearMonthWindSolar.columns.get_level_values(1)

# Output to CSV
tableFileName = "energyProductionWindSolarOverTimeDaily"
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotDateWindSolar.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")

tableFileName = "energyProductionWindSolarOverTimeDaily365Days"
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotDateWindSolar.tail(365).to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")

tableFileName = "energyProductionWindSolarOverTimeMonthly"
utils.saveFileInfo(scriptInfo, tableFileName)
dfPivotYearMonthWindSolar.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")
