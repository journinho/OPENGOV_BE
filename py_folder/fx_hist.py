import pandas as pd
import os


df_url = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip'
df = pd.read_csv(df_url, compression='zip', header=0, sep=',', decimal=".")

outputPath = "data/data_bewerkt/economy/wisselkoersen/"
# Create folder if it does not exist
if not os.path.exists(outputPath):
    print("Making directory", outputPath)
    os.makedirs(outputPath)

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
    df_temp.to_csv(f'{outputPath}fx_hist_EUR_' + key + '.csv', index=False)
