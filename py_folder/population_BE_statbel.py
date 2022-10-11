import pandas as pd

#import data
url = 'https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/TF_SOC_POP_STRUCT_2022.zip'
df = pd.read_csv(url, sep='|', encoding = "ISO-8859-1")
#delete columns in french
df = df.drop(['TX_ADM_DSTR_DESCR_FR','TX_DESCR_FR', 'TX_RGN_DESCR_FR', 'TX_PROV_DESCR_FR', 'TX_NATLTY_FR', 'TX_CIV_STS_FR' ], axis=1)


# # Leeftijdspiramide


leeftijd_per_geslacht = pd.pivot_table(df, index='CD_AGE', columns='CD_SEX', values='MS_POPULATION', aggfunc='sum').reset_index()
leeftijd_per_geslacht.rename(columns = {'CD_AGE':'Leeftijd', 'F':'Vrouw', 'M':'Man'}, inplace = True)
leeftijd_per_geslacht.set_index("Leeftijd", inplace=True)
leeftijd_per_geslacht.to_csv('data/data_bewerkt/population/Bevolkingsaantal_per_leeftijd_geslacht.csv')

# # Bevolkingsaantal per gemeente


bevolking_per_gemeente = pd.pivot_table(df, index=["TX_DESCR_NL", "CD_REFNIS"], values='MS_POPULATION', aggfunc='sum').reset_index()
bevolking_per_gemeente.rename(columns={"TX_DESCR_NL":"Gemeente", "MS_POPULATION": "Aantal", "CD_REFNIS":"NSI-code"}, inplace=True)
bevolking_per_gemeente.to_csv('data/data_bewerkt/population/bevolkingsaantal_per_gemeente.csv')



bevolking_per_gemeente_geslacht = pd.pivot_table(df, index=["TX_DESCR_NL", "CD_REFNIS"], columns="CD_SEX", values='MS_POPULATION', aggfunc='sum').reset_index()
bevolking_per_gemeente_geslacht.rename(columns={"TX_DESCR_NL":"Gemeente", "MS_POPULATION": "Aantal", "CD_REFNIS":"NSI-code", "F":"Vrouw", "M":"Man"}, inplace=True)
bevolking_per_gemeente_geslacht.to_csv('data/data_bewerkt/population/bevolkingsaantal_per_gemeente_geslacht.csv')
