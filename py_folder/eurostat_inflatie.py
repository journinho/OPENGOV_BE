import pandas as pd
import datetime
import os
import sys
import eurostat
import utils

# Metadata for this script
scriptInfo = {
    'scriptID': os.path.splitext(str(os.path.basename(__file__)))[0],
    'scriptPath': str(os.path.abspath(__file__)),
    'sourcePath': 'data/data_clean/eurostat/economy/',
    'outputPath': 'data/data_bewerkt/eurostat/economy/',
    'sources': [
        'https://ec.europa.eu/eurostat/databrowser/view/PRC_HICP_MANR__custom_3761882/bookmark/table?lang=en&bookmarkId=4ad27e6f-358a-4a3d-82a0-587d69a833eb'
        ],
    'sourcesExplanation': [
        'https://ec.europa.eu/eurostat/cache/metadata/en/prc_hicp_esms.htm'
        ]
}
utils.saveScriptInfo(scriptInfo)
# Create input and output folders if they do not exist
utils.createFolder(scriptInfo['sourcePath'])
utils.createFolder(scriptInfo['outputPath'])


# create a dictionary with land codes and country names of EU27 countries in Dutch
landcodes_EU = {'AT': 'Oostenrijk', 'BE': 'België', 'BG': 'Bulgarije', 'CY': 'Cyprus', 'CZ': 'Tsjechië', 'DE': 'Duitsland', 'DK': 'Denemarken', 'EE': 'Estland', 'ES': 'Spanje', 'FI': 'Finland', 'FR': 'Frankrijk', 'EL': 'Griekenland', 'HR': 'Kroatië', 'HU': 'Hongarije', 'IE': 'Ierland', 'IT': 'Italië', 'LT': 'Litouwen', 'LU': 'Luxemburg', 'LV': 'Letland', 'MT': 'Malta', 'NL': 'Nederland', 'PL': 'Polen', 'PT': 'Portugal', 'RO': 'Roemenië', 'SE': 'Zweden', 'SI': 'Slovenië', 'SK': 'Slowakije', 'EU27_2020': 'Europese Unie'}

# get keys from landcodes_EU dictionary
landcodes = list(landcodes_EU.keys())

# give the code for the indicator we want to download. In this case the inflation for the EU27 countries
code = 'PRC_HICP_MANR'
#create a dataframe with the indicator. 
df = eurostat.get_data_df(code)
df.to_csv(f"{scriptInfo['sourcePath']}PRC_HICP_MANR.csv")
# select only the total inflation for the EU27 countries
inflatie_eu = df.loc[df['coicop'] == 'CP00']


# drop irrelevant columns freq unit coicop
inflatie_eu.drop(columns = ['freq', 'unit', 'coicop'], inplace=True)

# keep the first and the last 120 columns from inflation_eu
plot_data = pd.concat([inflatie_eu.iloc[:, 0:1], inflatie_eu.iloc[:, -120:]], axis=1)
#remove EU28 from rows because the composition of the EU has changed over the years

#change name of column geo\time_period to Gebied
plot_data.rename(columns={'geo\\TIME_PERIOD':'Gebied'}, inplace=True)
# set the column 'Gebied' as index
plot_data.set_index('Gebied', inplace=True)

# filter out the EU27 countries
plot_data = plot_data.loc[landcodes]

#replace the landcodes with the country names
plot_data.rename(index=landcodes_EU, inplace=True)

# export the data to a csv file
tableFileName = 'inflatie_eu'
utils.saveFileInfo(scriptInfo, tableFileName)
plot_data.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")

# make new dataframe inflatie_bar where the index is the country name and the last column is the inflation rate of the last month. If value is NaN, then the value of the previous month is used
inflatie_bar = pd.DataFrame()
inflatie_bar['Inflatie'] = plot_data.iloc[:, -1]
inflatie_bar['Inflatie'] = inflatie_bar['Inflatie'].fillna(plot_data.iloc[:, -2])
inflatie_bar['Inflatie'] = inflatie_bar['Inflatie'].fillna(plot_data.iloc[:, -3])

#make a csv from inflatie_bar
tableFileName = 'inflatie_bar'
utils.saveFileInfo(scriptInfo, tableFileName)
inflatie_bar.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv")