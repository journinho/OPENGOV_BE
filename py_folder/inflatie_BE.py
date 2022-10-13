# %%
import pandas as pd
import os

outputPath = "data/data_bewerkt/economy/"
if not os.path.exists(outputPath):
    print("Making directory", outputPath)
    os.makedirs(outputPath)

# %%
df_url = "https://statbel.fgov.be/sites/default/files/files/opendata/Consumptieprijsindex%20en%20gezondheidsindex/CPI%20All%20base%20years.zip"
df = pd.read_csv(df_url, sep="|")

# %%
inflatie_be = pd.pivot_table(df, index=['NM_YR', 'NM_MTH'], values=['MS_CPI_INFL'], aggfunc='sum').reset_index()
#extract last 120 months:
inflatie_be = inflatie_be.iloc[-120:]
day = 1
inflatie_be['DATE']= pd.to_datetime(dict(year=inflatie_be.NM_YR, month=inflatie_be.NM_MTH, day=day), format="%d-%m-%Y")
inflatie_be['YR_MTH'] = inflatie_be['DATE'].dt.strftime('%Y-%m')
inflatie_be = inflatie_be[['YR_MTH','MS_CPI_INFL']]
inflatie_be.rename(columns={'YR_MTH':'Jaar-Maand', 'MS_CPI_INFL': 'Inflatie'}, inplace=True)
# Make Jaar-Maand to index:
inflatie_be = inflatie_be.set_index('Jaar-Maand')
# Export inflatie_be to .csv-file:
inflatie_be.to_csv('data/data_bewerkt/economy/inflatie_be.csv')

# %%


# %%



