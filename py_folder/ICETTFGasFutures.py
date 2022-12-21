import pandas as pd
import os
import utils
import json


# Metadata for this script
scriptInfo = {
    'scriptID': os.path.splitext(str(os.path.basename(__file__)))[0],
    'scriptPath': str(os.path.abspath(__file__)),
    'sourcePath': 'data/data_clean/energy/',
    'outputPath': 'data/data_bewerkt/energy/',
    'sources': [
        'https://www.theice.com/marketdata/DelayedMarkets.shtml?getContractsAsJson=&productId=4331&hubId=7979',
        'https://www.theice.com/marketdata/DelayedMarkets.shtml?getHistoricalChartDataAsJson=&historicalSpan=3&marketId='
        ],
    'sourcesExplanation': [
        'https://www.theice.com/products/27996665/Dutch-TTF-Gas-Futures/'
        ]
}
utils.saveScriptInfo(scriptInfo)
# Create input and output folders if they do not exist
utils.createFolder(scriptInfo['sourcePath'])
utils.createFolder(scriptInfo['outputPath'])


# Download file and store in sourcePath
url = scriptInfo['sources'][0]
fileName = "ICETTFGasFutures-contractPeriods.json"
utils.downloadFile(url, scriptInfo['sourcePath'], fileName, False)

# Get the most recent marketId. This id changes every month and is needed for the url in the next step.
with open(f"{scriptInfo['sourcePath']}{fileName}") as json_file:
    marketId = json.load(json_file)[0]['marketId']

url = scriptInfo['sources'][1] + str(marketId)
# Het veld 'sources' in database moet de link bevatten van meest recente data, dus daarin de huidige marketId invullen
scriptInfo['sources'][1] = url
fileName = "ICETTFGasFutures-nextMonth.json"
utils.downloadFile(url, scriptInfo['sourcePath'], fileName, False)
with open(f"{scriptInfo['sourcePath']}{fileName}") as json_file:
    gasFuturesJSON = json.load(json_file)['bars']
gasFuturesDF = pd.DataFrame(gasFuturesJSON, columns=['Datum', 'Prijs per MWh'])
gasFuturesDF['Datum'] = pd.to_datetime(gasFuturesDF['Datum'])


# Output to CSV
tableFileName = 'ICETTFGasFutures'
utils.saveFileInfo(scriptInfo, tableFileName)
gasFuturesDF.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv", index=False)
