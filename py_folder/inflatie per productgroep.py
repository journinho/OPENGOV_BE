import pandas as pd
import os
import utils

# Metadata for this script
scriptInfo = {
    'scriptID': os.path.splitext(str(os.path.basename(__file__)))[0],
    'scriptPath': str(os.path.abspath(__file__)),
    'sourcePath': 'data/data_clean/economy/',
    'outputPath': 'data/data_bewerkt/economy/',
    'sources': [
        'https://statbel.fgov.be/sites/default/files/files/opendata/Indexen%20per%20productgroep/CPI%20All%20groups.zip'
        ],
    'sourcesExplanation': [
        'https://statbel.fgov.be/nl/themas/consumptieprijsindex/consumptieprijsindex#figures'
        ]
}
utils.saveScriptInfo(scriptInfo)
# Create input and output folders if they do not exist
utils.createFolder(scriptInfo['sourcePath'])
utils.createFolder(scriptInfo['outputPath'])


url = scriptInfo['sources'][0]
df = pd.read_csv(url, sep='|', low_memory=False)

#df.drop(['TX_COICOP_FR_LVL1', 'TX_COICOP_EN_LVL1', 'TX_COICOP_FR_LVL2', 'TX_COICOP_EN_LVL2', 'TX_COICOP_FR_LVL3', 'TX_COICOP_EN_LVL3', 'TX_COICOP_FR_LVL4', 'TX_COICOP_EN_LVL4', 'NM_BASE_YR', 'CD_COICOP'], axis=1, inplace=True)

df['day'] = 1
df['DATE'] = pd.to_datetime(dict(year=df.NM_YR, month=df.NM_MTH, day=df.day), format="%Y-%m-%d")
df['YR_MTH'] = df['DATE'].dt.strftime('%Y-%m')

level4 = df[df["NM_CD_COICOP_LVL"] == 4] 

prijzen = pd.pivot_table(level4, index = 'TX_COICOP_NL_LVL4', columns = 'YR_MTH', values = 'MS_CPI_INFL', aggfunc='sum').reset_index()
prijzen.dropna(inplace=True)

inflatie_per_productgroep = pd.concat([prijzen.iloc[:,:1],prijzen.iloc[:,-120:]],axis=1) # Puts them together row wise
inflatie_per_productgroep.rename(columns={'TX_COICOP_NL_LVL4': 'Productgroep', 'YR_MTH': 'Index'}, inplace=True)
inflatie_per_productgroep['Inflatie'] = inflatie_per_productgroep.iloc[:,-1:]
# Output to CSV
tableFileName = 'inflatie_per_productgroep'
utils.saveFileInfo(scriptInfo, tableFileName)
inflatie_per_productgroep.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")





