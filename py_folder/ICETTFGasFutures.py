import pandas as pd
import datetime
import os
import json
import requests
import sys
import gzip

# Metadata for this script
scriptInfo = {
    'scriptID': os.path.splitext(str(os.path.basename(__file__)))[0],
    'scriptPath': str(os.path.abspath(__file__)),
    'outputPath': 'data/data_bewerkt/energy/',
    'sourcePath': 'data/data_clean/energy/',
    'sources': [
        'https://www.theice.com/marketdata/DelayedMarkets.shtml?getContractsAsJson=&productId=4331&hubId=7979',
        'https://www.theice.com/marketdata/DelayedMarkets.shtml?getHistoricalChartDataAsJson=&historicalSpan=3&marketId='
        ],
    'sourcesExplanation': [
        'https://www.theice.com/products/27996665/Dutch-TTF-Gas-Futures/'
        ],
    'tags': ['energie', 'Nederland'],
    'explanation': '''
# Gasprijzen voor Nederlands gas.

Prijzen voor 'Dutch TTF Gas Futures'.
'''
}

def downloadFile(url: str, filePath: str, fileName: str, asZipped: bool = True):
    """Download a file from url and save it, optionally as a gzipped file.

    :param url: url to download the file from
    :param fileName: filename to give the downloaded file
    :param asZipped: save it in gzipped form
    """
    try:
        r = requests.get(url, allow_redirects=True)
    except Exception as e:
        print(f"Downloading {fileName} failed: ", e)
        sys.exit(1)
    try:
        if asZipped:
            with gzip.open(f"{filePath}{fileName}.gz", 'wb') as f:
                f.write(r.content)
        else:
            with open(f"{filePath}{fileName}", 'wb') as f:
                f.write(r.content)
    except Exception as e:
        print(f"Saving file {fileName} failed: ", e)
        sys.exit(1)

# Create folder if it does not exist
if not os.path.exists(scriptInfo['outputPath']):
    print("Making directory", scriptInfo['outputPath'])
    os.makedirs(scriptInfo['outputPath'])
if not os.path.exists(scriptInfo['sourcePath']):
    print("Making directory", scriptInfo['sourcePath'])
    os.makedirs(scriptInfo['sourcePath'])

# Download file and store in sourcePath
url = scriptInfo['sources'][0]
fileName = "ICETTFGasFutures-contractPeriods.json"
downloadFile(url, scriptInfo['sourcePath'], fileName, False)
# Get the most recent marketId. This id changes every month and is needed for the url in the next step.
with open(f"{scriptInfo['sourcePath']}{fileName}") as json_file:
    marketId = json.load(json_file)[0]['marketId']

url = scriptInfo['sources'][1] + str(marketId)
# Het veld 'sources' in database moet de link bevatten van meest recente data, dus daarin de huidige marketId invullen
scriptInfo['sources'][1] = url
fileName = "ICETTFGasFutures-nextMonth.json"
downloadFile(url, scriptInfo['sourcePath'], fileName, False)
with open(f"{scriptInfo['sourcePath']}{fileName}") as json_file:
    gasFuturesJSON = json.load(json_file)['bars']
gasFuturesDF = pd.DataFrame(gasFuturesJSON, columns=['Datum', 'Prijs per MWh'])
gasFuturesDF['Datum'] = pd.to_datetime(gasFuturesDF['Datum'])

# Output to CSV
tableFileName = 'ICETTFGasFutures'
gasFuturesDF.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv", index=False)
