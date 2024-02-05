import pandas as pd
import os
import utils

# Metadata for this script
scriptInfo = {
    'scriptID': os.path.splitext(str(os.path.basename(__file__)))[0],
    'scriptPath': str(os.path.abspath(__file__)),
    'sourcePath': 'data/data_clean/economy/wisselkoersen/',
    'outputPath': 'data/data_bewerkt/economy/wisselkoersen/',
    'sources': [
        'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip'
        ],
    'sourcesExplanation': [
        'https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html'
        ],
    'sourcesName': ['ECB']
}
utils.saveScriptInfo(scriptInfo)
# Create input and output folders if they do not exist
utils.createFolder(scriptInfo['sourcePath'])
utils.createFolder(scriptInfo['outputPath'])


df_url = scriptInfo['sources'][0]
df = pd.read_csv(df_url, compression='zip', header=0, sep=',', decimal=".")


# sort data by date
df = df.sort_values(by=['Date'])
# keep only last 5 years
df = df.tail(5*365)

# create dictionary with cols as keys and currency names as values in dutch
currency_dict = {'USD':'US dollar', 'JPY':'Japanse yen', 'BGN':'Bulgaarse lev', 'CZK':'Tsjechische kroon', 'DKK':'Deense kroon', 'GBP':'Britse pond', 'HUF':'Hongaarse forint', 'PLN':'Poolse zloty', 'RON':'Roemeense leu', 'SEK':'Zweedse kroon', 'CHF':'Zwitserse frank', 'ISK':'IJslandse kroon', 'NOK':'Noorse kroon', 'HRK':'Kroatische kuna', 'RUB':'Russische roebel', 'TRY':'Turkse lira', 'AUD':'Australische dollar', 'BRL':'Braziliaanse real', 'CAD':'Canadese dollar', 'CNY':'Chinese yuan renminbi', 'HKD':'Hongkongse dollar', 'IDR':'Indonesische roepia', 'ILS':'Israëlische shekel', 'INR':'Indiase roepie', 'KRW':'Zuid-Koreaanse won', 'MXN':'Mexicaanse peso', 'MYR':'Maleisische ringgit', 'NZD':'Nieuw-Zeelandse dollar', 'PHP':'Filipijnse peso', 'SGD':'Singaporese dollar', 'THB':'Thaise baht', 'ZAR':'Zuid-Afrikaanse rand'}

# for every key in currency_dict, create a new dataframe with date and key as columns
for key in currency_dict:
    # create new dataframe with date and key as columns
    df_temp = df[['Date', key]]
    # rename key column to currency name
    df_temp = df_temp.rename(columns={key:currency_dict[key]})
    # set date as index
    # Output to CSV
    tableFileName = f'fx_hist_EUR_{key}'
    utils.saveFileInfo(scriptInfo, tableFileName)
    df_temp.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv", index=False)