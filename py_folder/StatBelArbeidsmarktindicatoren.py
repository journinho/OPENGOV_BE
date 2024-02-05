import pandas as pd
import os
import utils
import datetime

# Metadata for this script
scriptInfo = {
    'scriptID': os.path.splitext(str(os.path.basename(__file__)))[0],
    'scriptPath': str(os.path.abspath(__file__)),
    'sourcePath': 'data/data_clean/economy/statbel/',
    'outputPath': 'data/data_bewerkt/economy/statbel/',
    'sources': [
        'https://statbel.fgov.be/sites/default/files/files/documents/Werk%20%26%20opleiding/9.1%20Lonen%20en%20arbeidskosten/Downloadbare%20tabel%20met%20kwartaalresultaten%20EAK%20vanaf%202017_NL.xls'
        ],
    'sourcesExplanation': [
        'https://statbel.fgov.be/nl/themas/werk-opleiding/arbeidsmarkt/werkgelegenheid-en-werkloosheid#figures'
        ],
    'sourcesName': ['StatBel']
}
utils.saveScriptInfo(scriptInfo)
# Create input and output folders if they do not exist
utils.createFolder(scriptInfo['sourcePath'])
utils.createFolder(scriptInfo['outputPath'])

# Download file and store in sourcePath
url = scriptInfo['sources'][0]
fileName = "Downloadbare tabel met kwartaalresultaten EAK vanaf 2017_NL.xls"
utils.downloadFile(url, scriptInfo['sourcePath'], fileName, False)


# Read sheet with indicators for ages 20-64
df = pd.read_excel(f"{scriptInfo['sourcePath']}{fileName}", sheet_name=" indicatoren 20-64")


# The last rows contain footnotes. Remove them.
df = df.head(41)
# Remove empty columns before (and in the middle of the table)
df = df[df.columns[df.head(1).notna().all()]]

# Replace column titles with dates (in the middle of the quarter)
lastColumnTitle = ""
newColumnTitles = []
for columnTitle in df.columns:
    if "kwartaal" in columnTitle:
        try:
            # Year
            year = int(columnTitle.strip().split(" ")[2])
            # Quarter
            quarter = int(list(columnTitle.strip().split(" ")[0])[0])
            # Date of middle of this quarter should be new title
            columnTitle = datetime.date(year, quarter * 3 - 1, 15)
            lastColumnTitle = columnTitle
            newColumnTitles.append(columnTitle)
        except:
            print(f"ERROR: |{columnTitle}| could not be transformed into a date.")
    else:
        newColumnTitles.append(lastColumnTitle)
df.columns = newColumnTitles
df.head()


# Select the columns with ages 20-54 (every 3rd column, starting from 0)
# Drop the first two rows
df2054 = df.iloc[2:, list(range(0, len(df.columns), 3))]
# Select the columns with ages 55-64 (every 3rd column, starting from 1)
# Drop the first two rows
df5564 = df.iloc[2:, list(range(1, len(df.columns), 3))]
# Select the columns with ages 20-64 (every 3rd column, starting from 2)
# Drop the first two rows
df2064 = df.iloc[2:, list(range(2, len(df.columns), 3))]


# Werkgelegenheid per regio
gewestNamen = ["België","Brussels Hoofdstedelijk Gewest","Vlaams Gewest", "Waals Gewest"]
# 20-54 jaar
df2054Werkgelegenheid = df2054.iloc[[2,12,22,32]]
df2054Werkgelegenheid.index = gewestNamen
tableFileName = 'StatBelArbeidsmarktindicatoren2054Werkgelegenheid'
utils.saveFileInfo(scriptInfo, tableFileName)
df2054Werkgelegenheid.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")
# 55-64 jaar
df5564Werkgelegenheid = df5564.iloc[[2,12,22,32]]
df5564Werkgelegenheid.index = gewestNamen
tableFileName = 'StatBelArbeidsmarktindicatoren5564Werkgelegenheid'
utils.saveFileInfo(scriptInfo, tableFileName)
df5564Werkgelegenheid.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")
# 20-64 jaar
df2064Werkgelegenheid = df2064.iloc[[2,12,22,32]]
df2064Werkgelegenheid.index = gewestNamen
tableFileName = 'StatBelArbeidsmarktindicatoren2064Werkgelegenheid'
utils.saveFileInfo(scriptInfo, tableFileName)
df2064Werkgelegenheid.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")


# Activiteitsgraad per regio
# 20-54 jaar
df2054Activiteitsgraad = df2054.iloc[[5,15,25,35]]
df2054Activiteitsgraad.index = gewestNamen
tableFileName = 'StatBelArbeidsmarktindicatoren2054Activiteitsgraad'
utils.saveFileInfo(scriptInfo, tableFileName)
df2054Activiteitsgraad.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")
# 55-64 jaar
df5564Activiteitsgraad = df5564.iloc[[5,15,25,35]]
df5564Activiteitsgraad.index = gewestNamen
tableFileName = 'StatBelArbeidsmarktindicatoren5564Activiteitsgraad'
utils.saveFileInfo(scriptInfo, tableFileName)
df5564Activiteitsgraad.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")
# 20-64 jaar
df2064Activiteitsgraad = df2064.iloc[[5,15,25,35]]
df2064Activiteitsgraad.index = gewestNamen
tableFileName = 'StatBelArbeidsmarktindicatoren2064Activiteitsgraad'
utils.saveFileInfo(scriptInfo, tableFileName)
df2064Activiteitsgraad.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")


# Werkloosheidsgraad per regio
# 20-54 jaar
df2054Werkloosheidsgraad = df2054.iloc[[8,18,28,38]]
df2054Werkloosheidsgraad.index = gewestNamen
tableFileName = 'StatBelArbeidsmarktindicatoren2054Werkloosheidsgraad'
utils.saveFileInfo(scriptInfo, tableFileName)
df2054Werkloosheidsgraad.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")
# 55-64 jaar
df5564Werkloosheidsgraad = df5564.iloc[[8,18,28,38]]
df5564Werkloosheidsgraad.index = gewestNamen
tableFileName = 'StatBelArbeidsmarktindicatoren5564Werkloosheidsgraad'
utils.saveFileInfo(scriptInfo, tableFileName)
df5564Werkloosheidsgraad.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")
# 20-64 jaar
df2064Werkloosheidsgraad = df2064.iloc[[8,18,28,38]]
df2064Werkloosheidsgraad.index = gewestNamen
tableFileName = 'StatBelArbeidsmarktindicatoren2064Werkloosheidsgraad'
utils.saveFileInfo(scriptInfo, tableFileName)
df2064Werkloosheidsgraad.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")


# Werkloosheidsgraad België per leeftijd
dfWerkgelegenheidsAges = pd.concat([df2054Werkgelegenheid.loc["België"], df5564Werkgelegenheid.loc["België"], df2064Werkgelegenheid.loc["België"]], axis=1)
dfWerkgelegenheidsAges.columns = ["20-54", "55-64", "20-64"]
tableFileName = 'StatBelArbeidsmarktindicatorenWerkgelegenheidAges'
utils.saveFileInfo(scriptInfo, tableFileName)
dfWerkgelegenheidsAges.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")

# Activiteitsgraad België per leeftijd
dfActiviteitsgraadAges = pd.concat([df2054Activiteitsgraad.loc["België"], df5564Activiteitsgraad.loc["België"], df2064Activiteitsgraad.loc["België"]], axis=1)
dfActiviteitsgraadAges.columns = ["20-54", "55-64", "20-64"]
tableFileName = 'StatBelArbeidsmarktindicatorenActiviteitsgraadAges'
utils.saveFileInfo(scriptInfo, tableFileName)
dfActiviteitsgraadAges.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")

# Werkloosheidsgraad België per leeftijd
dfWerkloosheidsgraadAges = pd.concat([df2054Werkloosheidsgraad.loc["België"], df5564Werkloosheidsgraad.loc["België"], df2064Werkloosheidsgraad.loc["België"]], axis=1)
dfWerkloosheidsgraadAges.columns = ["20-54", "55-64", "20-64"]
tableFileName = 'StatBelArbeidsmarktindicatorenWerkloosheidsgraadAges'
utils.saveFileInfo(scriptInfo, tableFileName)
dfWerkloosheidsgraadAges.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")
