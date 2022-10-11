'''
Version 1.0 - 2018
#Author: Marco Scarselli
#DataLifeLab 
MIT LICENSE
'''


import pandas as pd
import numpy as np
from itertools import compress

def tsv_to_dataframe(file_tsv):
    '''
    this function transforms Eurostat tsv file in pandas dataframe
    file_tsv: file name. It's work with tsv and compressed file "tsv.gz"
    '''
    
    def clean_cells(x):
        '''This function transforms Eurostat Missing Values ": " in numpy missing values.
        Then clean Eurostat annotation "b, u, .."'''
        try:
            return float(x)
        except:
            try:
                return float(x.split(" ")[0])
            except:
                return np.nan
    # open the Eurostat TSV file 

    data = pd.read_csv(file_tsv, sep="\t")
    # Create a dataframe for values data
    data_clean = data
    # Clean data values with clean_cells function
    data_clean = data_clean.applymap(lambda x: clean_cells(x))
    # Drop column with variable name like "age,isced11,unit,sex,geo\time". It is the first column. we have a 
    # dataframe with only data values 
    data_clean.drop(data_clean.columns[0], axis = 1, inplace = True)
    # transform column with variable in multiple-columns  
    variabili = data[data.columns[0]].apply(lambda x: pd.Series(x.split(",")))
    variabili.columns = data.columns[0].split(",")
    # return cleaned dataframe in pandas dataframe
    return pd.concat([variabili, data_clean], axis = 1)



def tsv_to_dataframe_long(file_tsv, structure = "normal"):
    '''
    this function transforms Eurostat tsv file in pandas dataframe
    file_tsv: file name. It's work with tsv and compressed file "tsv.gz"
    structure: "normal", columns indicates time
    structure: "inverse", columns indicates Nuts / geo
    
    '''
    
    def clean_cells(x):
        '''This function transforms Eurostat Missing Values ": " in numpy missing values.
        Then clean Eurostat annotation "b, u, .."'''
        try:
            return float(x)
        except:
            try:
                return float(x.split(" ")[0])
            except:
                return np.nan
            
    def annotation(x):
            '''This function extracts Eurostat annotation "b, u, .."'''
            try:
                return x.split(" ")[1]
            except:
                return np.nan

    def columns_type(x):
            try:
                return int(x.strip())
            except:
                if x == "geo\\time":
                    return "var_" + "geo"
                else:
                    return "var_" + x
                
    
    def columns_type_inverse(columns):
        new_columns =[]
        sep = columns.get_loc("time\\geo")
        new_columns.extend(list(columns[:sep].map(lambda x: "var_" + x)))
        new_columns.append("var_time")
        new_columns.extend(list(columns[sep + 1:]))
        return new_columns
    
    # open the Eurostat TSV file 

    data = pd.read_csv(file_tsv, sep="\t")
    # Create a dataframe for values data
    data_clean = data
    # Clean data values with clean_cells function
    data_clean = data_clean.applymap(lambda x: x)
    # Drop column with variable name like "age,isced11,unit,sex,geo\time". It is the first column. we have a 
    # dataframe with only data values 
    data_clean.drop(data_clean.columns[0], axis = 1, inplace = True)
    # transform column with variable in multiple-columns  
    variabili = data[data.columns[0]].apply(lambda x: pd.Series(x.split(",")))
    variabili.columns = data.columns[0].split(",")
    # return cleaned dataframe in pandas dataframe
    result = pd.concat([variabili, data_clean], axis = 1)
    
    if structure == "normal":
        colonne  = list(map(lambda x: columns_type(x), list(result.columns.values)))
        result.columns = colonne
        colonne_var = result.columns.map(lambda x: str(x)[0:3] == "var")
        index = list(compress(result.columns, colonne_var))
        result = result.melt(id_vars = index)
        result["value_raw"] = result["value"]
        result["eurostat_annotation"] = result["value_raw"].apply(lambda x: annotation(x))
        result["value"] = result["value"].apply(clean_cells) 
    elif structure == "inverse":
        result.columns = columns_type_inverse(result.columns)
        colonne_var = result.columns.map(lambda x: str(x)[0:3] == "var")
        index = list(compress(result.columns, colonne_var))
        result = result.melt(id_vars = index, var_name = "geo")
        result["value_raw"] = result["value"]
        result["eurostat_annotation"] = result["value_raw"].apply(lambda x: annotation(x))
        result["value"] = result["value"].apply(clean_cells) 
        
    
    return result
#example 
#if __name__ == "__main__":
import urllib.request
import gzip

eurostat_link = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data%2Faact_eaa07.tsv.gz"
urllib.request.urlretrieve(eurostat_link , "file.tsv.gz")


result = tsv_to_dataframe("file.tsv.gz")
result