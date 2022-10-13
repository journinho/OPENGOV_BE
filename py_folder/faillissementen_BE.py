# %%
import pandas as pd

# %%
df_url = ('https://statbel.fgov.be/sites/default/files/files/opendata/BRI_Nace/TF_BANKRUPTCIES.zip')
df = pd.read_csv(df_url, sep="|")
df = df.drop(['TX_EMPLOYMENT_CLASS_DESCR_FR', 'TX_LEGAL_FORM_DESCR_FR', 'TX_MUNTY_DESCR_FR', 'TX_ADM_DSTR_DESCR_FR',
             'TX_PROV_DESCR_FR', 'TX_RGN_DESCR_FR', 'TX_NACE_REV2_SECTION_FR', 'TX_NACE_REV2_CLASS_FR', 'TX_NACE_REV2_GROUP_FR', 'TX_NACE_REV2_DIVISION_FR'], axis=1)
df.head()


# %%
faillissementen_per_maand = pd.pivot_table(df, index=["CD_YEAR", "CD_MONTH"], values='MS_COUNTOF_BANKRUPTCIES', aggfunc="sum").reset_index()
day = 1
faillissementen_per_maand['DATE'] = pd.to_datetime({'day': day, 'month': faillissementen_per_maand.CD_MONTH, 'year': faillissementen_per_maand.CD_YEAR})
# extract last 120 months:
faillissementen_per_maand = faillissementen_per_maand .iloc[-120:]
# Get year-month:
faillissementen_per_maand['Jaar-Maand'] = faillissementen_per_maand['DATE'].dt.strftime('%Y-%m')
faillissementen_per_maand = faillissementen_per_maand[['Jaar-Maand','MS_COUNTOF_BANKRUPTCIES']]

faillissementen_per_maand.rename(columns={'MS_COUNTOF_BANKRUPTCIES':'Aantal faillissementen'}, inplace=True)
# Make 'Jaar-Maand' to index:
faillissementen_per_maand=faillissementen_per_maand.set_index('Jaar-Maand')
# Export data to .csv-file:
faillissementen_per_maand.to_csv('data/data_bewerkt/economy/faillissementen_per_maand.csv')
  


# %%



