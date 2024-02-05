import pandas as pd
import os
import utils
import json
import markdownify

# Metadata for this script
scriptInfo = {
    'scriptID': os.path.splitext(str(os.path.basename(__file__)))[0],
    'scriptPath': str(os.path.abspath(__file__)),
    'sourcePath': 'data/data_clean/mobiliteit/',
    'outputPath': 'data/data_bewerkt/mobiliteit/',
    'sources': [
        'https://opendata.infrabel.be/api/explore/v2.1/'
        ],
    'sourcesExplanation': [
        'https://opendata.infrabel.be/explore/?disjunctive.keyword&sort=explore.popularity_score'
        ],
    'sourcesName': ['Infrabel'],
    'description': 'Data over het treinverkeer in België, afkomstig van het Open Data portaal van Infrabel.',
    'tags': ['België', 'Mobiliteit'],
}
utils.saveScriptInfo(scriptInfo)

# Create input and output folders if they do not exist
utils.createFolder(scriptInfo['sourcePath'])
utils.createFolder(scriptInfo['outputPath'])

datasets = pd.read_csv('input/infrabel-datasets-export.csv', sep=';')

# Download files and store in sourcePath
for index, dataset in datasets.iterrows():
    dataset_id = dataset['datasetid']
    # Download files and store in sourcePath
    url = scriptInfo['sources'][0] + '/catalog/datasets/' + dataset_id + '/exports/json'
    filename = f"Infrabel-{dataset_id}.json"
    utils.downloadFile(url, scriptInfo['sourcePath'], fileName, False)

def save_to_csv(scriptInfo, df, tableFileName, description='', tags=[]):
    # print(tableFileName)
    # Save result as CSV file
    df.to_csv(f"{scriptInfo['outputPath']}{tableFileName}.csv", index=False)    
    # Register in NWSify
    utils.saveFileInfo(scriptInfo, tableFileName, description, tags)

# Loop over all datasets and pass them to infrabel_transform() to do dataset specific transformations
for index, dataset in datasets.iterrows():
    dataset_id = dataset['datasetid']
    filename = f"Infrabel-{dataset_id}.json"
    df_ori = pd.DataFrame(json.loads(open(scriptInfo['sourcePath'] + filename).read()))
    
    # Do dataset specific stuff that leads to df_final.
    if dataset_id == 'evolutie-van-het-aantal-overwegen':
        df_final = pd.pivot_table(df_ori, index=['jaar'], values='aantal', columns=['type_lijn_nl'], aggfunc='sum').reset_index()
        try:
            description = markdownify.markdownify(dataset['default.description'])
        except:
            description = ''
        tags = []
        tableFileName = f"Infrabel-{dataset_id}"
        # Save result as CSV file and register in NWSify
        save_to_csv(scriptInfo, df_final, tableFileName, description, tags)

    elif dataset_id == 'evolutie-van-het-aantal-overwegen-per-area':
        df_final = pd.pivot_table(df_ori, index=['jaar'], values='aantal', columns=['area'], aggfunc='sum').reset_index()
        try:
            description = markdownify.markdownify(dataset['default.description'])
        except:
            description = ''
        tags = []
        tableFileName = f"Infrabel-{dataset_id}-area"
        # Save result as CSV file and register in NWSify
        save_to_csv(scriptInfo, df_final, tableFileName, description, tags)

        df_final = pd.pivot_table(df_ori, index=['jaar'], values='aantal', columns=['overwegcategorie'], aggfunc='sum').reset_index()
        try:
            description = markdownify.markdownify(dataset['default.description'])
        except:
            description = ''
        tags = []
        tableFileName = f"Infrabel-{dataset_id}-overwegcategorie"
        # Save result as CSV file and register in NWSify
        save_to_csv(scriptInfo, df_final, tableFileName, description, tags)

    elif dataset_id == 'evolutie-van-het-aantal-overwegen-per-gewest':
        df_final = pd.pivot_table(df_ori, index=['jaar'], values='aantal', columns=['gewest_nl'], aggfunc='sum').reset_index()
        try:
            description = markdownify.markdownify(dataset['default.description'])
        except:
            description = ''
        tags = []
        tableFileName = f"Infrabel-{dataset_id}-gewest"
        # Save result as CSV file and register in NWSify
        save_to_csv(scriptInfo, df_final, tableFileName, description, tags)

    elif dataset_id == 'aantal-opleidingsdagen':
        df_final = pd.pivot_table(df_ori, index=['q'], values='data', columns=[], aggfunc='sum').reset_index()
        df_final['q'] = pd.to_datetime(df_final['q'], format='%Y-%m')
        # Take middle of the month as date
        df_final['q'] = df_final['q'] + pd.DateOffset(days=14)
        df_final.columns = ['maand', 'opleidingsdagen']
        try:
            description = markdownify.markdownify(dataset['default.description'])
        except:
            description = ''
        tags = []
        tableFileName = f"Infrabel-{dataset_id}-opleidingsdagen"
        # Save result as CSV file and register in NWSify
        save_to_csv(scriptInfo, df_final, tableFileName, description, tags)

        df_final = pd.pivot_table(df_ori, index=['q'], values='data', columns=[], aggfunc='sum').reset_index()
        df_final['q'] = pd.to_datetime(df_final['q'], format='%Y-%m')
        # Take middle of the month as date
        df_final['q'] = df_final['q'] + pd.DateOffset(days=14)
        df_final.columns = ['maand', 'medewerkers']
        try:
            description = markdownify.markdownify(dataset['default.description'])
        except:
            description = ''
        tags = []
        tableFileName = f"Infrabel-{dataset_id}-opgeleide-medewerkers"
        # Save result as CSV file and register in NWSify
        save_to_csv(scriptInfo, df_final, tableFileName, description, tags)

    elif dataset_id == 'afgeschafte-overwegen':
        df_final = pd.pivot_table(df_ori, index=['jaar'], values='aantal', columns=['openbaar_prive_nl'], aggfunc='sum').reset_index()
        df_final.columns = ['maand', 'openbaar', 'prive']
        try:
            description = markdownify.markdownify(dataset['default.description'])
        except:
            description = ''
        tags = []
        tableFileName = f"Infrabel-{dataset_id}"
        # Save result as CSV file and register in NWSify
        save_to_csv(scriptInfo, df_final, tableFileName, description, tags)

    elif dataset_id == 'afgeschafte-treinen-per-maand-vanaf-2020':
        df_final = df_ori.copy()
        
        df_final.columns = ['maand', 'Totaal afgeschafte treinen', 'Aantal gedeeltelijk afgeschafte treinen', 'Aantal volledig afgeschafte treinen', 'Aantal treinen', 'Percentage afgeschafte treinen', 'Jaar']
        df_final.drop(columns=['Jaar'], inplace=True)
        try:
            description = markdownify.markdownify(dataset['default.description'])
        except:
            description = ''
        tags = []
        tableFileName = f"Infrabel-{dataset_id}"
        # Save result as CSV file and register in NWSify
        save_to_csv(scriptInfo, df_final, tableFileName, description, tags)

    elif dataset_id == 'belangrijkste-incidenten':
        df_temp = df_ori.copy()
        df_temp['Jaar'] = pd.to_datetime(df_temp['date']).dt.year
        df_final = pd.pivot_table(df_temp, index=['Jaar'], values='min_delay', columns=['aard_van_incident'], aggfunc='sum').reset_index()
        # Replace NaN values with 0
        df_final = df_final.fillna(0)
        # Add totaal column
        df_final['Totaal'] = df_final.sum(axis=1)
        try:
            description = markdownify.markdownify(dataset['default.description'])
        except:
            description = ''
        tags = []
        tableFileName = f"Infrabel-{dataset_id}-minuten-vertraging"
        # Save result as CSV file and register in NWSify
        save_to_csv(scriptInfo, df_final, tableFileName, description, tags)

        df_final = pd.pivot_table(df_temp, index=['Jaar'], values='sup', columns=['aard_van_incident'], aggfunc='sum').reset_index()
        # Replace NaN values with 0
        df_final = df_final.fillna(0)
        # Add totaal column
        df_final['Totaal'] = df_final.sum(axis=1)
        try:
            description = markdownify.markdownify(dataset['default.description'])
        except:
            description = ''
        tags = []
        tableFileName = f"Infrabel-{dataset_id}-vertraagde-treinen"
        # Save result as CSV file and register in NWSify
        save_to_csv(scriptInfo, df_final, tableFileName, description, tags)

    elif dataset_id == 'evolutie-van-treinkilometers':
        df_temp = df_ori.copy()
        # Only count actual km driven (not planned km)
        df_temp = df_temp[df_temp['effectief_niet_effectief_nl'] == 'Effectief'].reset_index(drop=True)

        # Add a column with the date based on 'trimester' column.
        # The format of the trimester is '2007-Q1', so we need to split the string and add the first part to the year column and the second part to the quarter column.
        df_temp[['year', 'quarter']] = df_temp['trimester'].str.split('-', expand=True)
        # Convert the quarter column to a numeric value
        df_temp['quarter'] = [int(x.strip().replace('Q', '')) for x in df_temp['quarter']]
        df_temp['Datum'] = pd.to_datetime([f"{row['jaar']}-{row['quarter'] * 3 - 1}-15" for index, row in df_temp.iterrows()])
        df_temp.drop(columns=['jaar', 'trimester', 'year', 'quarter', 'effective_', 'effectief_niet_effectief_fr', 'effectief_niet_effectief_nl', 'treintype_en', 'sector_fr'], inplace=True)


        df_final = pd.pivot_table(df_temp, index=['Datum'], values='trein_km', columns=['sector_nl'], aggfunc='sum').reset_index()

        # Replace NaN values with 0
        df_final = df_final.fillna(0)
        # Set the index to the date column so the next line ignores the date when getting rid of negative values
        df_final.set_index('Datum', inplace=True)
        # If a value is negative, make it zero (because one train seems to have driven negative 5km)
        df_final[df_final < 0] = 0
        # Turn index into a column again
        df_final.reset_index(inplace=True)

        try:
            description = markdownify.markdownify(dataset['default.description'])
        except:
            description = ''
        tags = []
        tableFileName = f"Infrabel-{dataset_id}"
        # Save result as CSV file and register in NWSify
        save_to_csv(scriptInfo, df_final, tableFileName, description, tags)

    elif dataset_id == 'evolutie-van-het-aantal-effectieve-rijpaden':
        df_final = df_ori.copy()
        df_final.drop(columns=['categorie_fr', 'treintype_en'], inplace=True)
        # Add a column with the date based on 'trimester' column.
        # The format of the trimester is '2007-Q1', so we need to split the string and add the first part to the year column and the second part to the quarter column.
        df_final[['year', 'quarter']] = df_final['trimester'].str.split('-', expand=True)

        # Convert the quarter column to a numeric value
        df_final['quarter'] = [int(x.strip().replace('Q', '')) for x in df_final['quarter']]

        df_final['Datum'] = pd.to_datetime([f"{row['jaar']}-{row['quarter'] * 3 - 1}-15" for index, row in df_final.iterrows()])
        # df_final.drop(columns=['jaar', 'trimester', 'year', 'quarter', 'effective_', 'effectief_niet_effectief_fr', 'effectief_niet_effectief_nl', 'treintype_en', 'sector_fr'], inplace=True)

        df_final.drop(columns=['jaar', 'trimester', 'year', 'quarter'], inplace=True)
        df_final = pd.pivot_table(df_final, index=['Datum'], values='aantal_effectieve_rijpaden', columns=['categorie_nl'], aggfunc='sum').reset_index()

        # Replace NaN values with 0
        df_final = df_final.fillna(0)

        # Set the index to the date column
        df_final.set_index('Datum', inplace=True)

        # If value is negative, make it zero
        df_final[df_final < 0] = 0
        # Turn index into a column again
        df_final.reset_index(inplace=True)

        try:
            description = markdownify.markdownify(dataset['default.description'])
        except:
            description = ''
        tags = []
        tableFileName = f"Infrabel-{dataset_id}"
        # Save result as CSV file and register in NWSify
        save_to_csv(scriptInfo, df_final, tableFileName, description, tags)

    elif dataset_id == 'indicator_modalshift':
        df_final = df_ori.copy().drop(columns=['type_de_transport', 'type_transport'])
        df_final.columns = ['Jaar', 'Vervoersmiddel', 'Nettotonnage in duizend ton']
        df_final = pd.pivot_table(df_final, index=['Jaar'], values='Nettotonnage in duizend ton', columns=['Vervoersmiddel'], aggfunc='sum').reset_index()
        
        try:
            description = markdownify.markdownify(dataset['default.description'])
        except:
            description = ''
        tags = []
        tableFileName = f"Infrabel-{dataset_id}"
        # Save result as CSV file and register in NWSify
        save_to_csv(scriptInfo, df_final, tableFileName, description, tags)

