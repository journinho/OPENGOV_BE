import pandas as pd
import os
import utils

# Metadata for this script
scriptInfo = {
    'scriptID': os.path.splitext(str(os.path.basename(__file__)))[0],
    'scriptPath': str(os.path.abspath(__file__)),
    'sourcePath': 'data/data_clean/population/',
    'outputPath': 'data/data_bewerkt/population/',
    'sources': [
        'https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/TF_SOC_POP_STRUCT_2022.zip'
        ],
    'sourcesExplanation': [
        'https://statbel.fgov.be/nl/themas/bevolking/structuur-van-de-bevolking#figures'
        ]
}
utils.saveScriptInfo(scriptInfo)
# Create input and output folders if they do not exist
utils.createFolder(scriptInfo['sourcePath'])
utils.createFolder(scriptInfo['outputPath'])





#import data
url = scriptInfo['sources'][0]
df = pd.read_csv(url, sep='|', encoding = "ISO-8859-1")
#delete columns in french
df = df.drop(['TX_ADM_DSTR_DESCR_FR','TX_DESCR_FR', 'TX_RGN_DESCR_FR', 'TX_PROV_DESCR_FR', 'TX_NATLTY_FR', 'TX_CIV_STS_FR' ], axis=1)


# # Leeftijdspiramide


leeftijd_per_geslacht = pd.pivot_table(df, index='CD_AGE', columns='CD_SEX', values='MS_POPULATION', aggfunc='sum').reset_index()
leeftijd_per_geslacht.rename(columns = {'CD_AGE':'Leeftijd', 'F':'Vrouw', 'M':'Man'}, inplace = True)
leeftijd_per_geslacht.set_index("Leeftijd", inplace=True)
# Output to CSV
tableFileName = 'Bevolkingsaantal_per_leeftijd_geslacht'
utils.saveFileInfo(scriptInfo, tableFileName)
leeftijd_per_geslacht.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")

# # Bevolkingsaantal per gemeente


bevolking_per_gemeente = pd.pivot_table(df, index=["TX_DESCR_NL", "CD_REFNIS"], values='MS_POPULATION', aggfunc='sum').reset_index()
bevolking_per_gemeente.rename(columns={"TX_DESCR_NL":"Gemeente", "MS_POPULATION": "Aantal", "CD_REFNIS":"NSI-code"}, inplace=True)
# Output to CSV
tableFileName = 'bevolkingsaantal_per_gemeente'
utils.saveFileInfo(scriptInfo, tableFileName)
bevolking_per_gemeente.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")



bevolking_per_gemeente_geslacht = pd.pivot_table(df, index=["TX_DESCR_NL", "CD_REFNIS"], columns="CD_SEX", values='MS_POPULATION', aggfunc='sum').reset_index()
bevolking_per_gemeente_geslacht.rename(columns={"TX_DESCR_NL":"Gemeente", "MS_POPULATION": "Aantal", "CD_REFNIS":"NSI-code", "F":"Vrouw", "M":"Man"}, inplace=True)
# Output to CSV
tableFileName = 'bevolkingsaantal_per_gemeente_geslacht'
utils.saveFileInfo(scriptInfo, tableFileName)
bevolking_per_gemeente_geslacht.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")
