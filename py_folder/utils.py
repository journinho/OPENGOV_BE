import requests
import sys
import gzip
import json
import os
from dotenv import load_dotenv

load_dotenv("./.env")


def createFolder(path: str):
    """Create a folder if it does not exist yet.
    
    :param path: path of the folder
    """
    if not os.path.exists(path):
        print("Making directory", path)
        os.makedirs(path)


def downloadFile(url: str, filePath: str, fileName: str, asZipped: bool = True):
    """Download a file from url and save it, optionally as a gzipped file.

    :param url: url to download the file from
    :param filePath: path where the downloaded file should be stored
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


def saveScriptInfo(scriptInfo: dict):
    """Save script info in a database

    :param scriptInfo: Dictionary with information about this script.
    """

    url = os.getenv('apiURLScript')
    data = {
        'apiKey': os.getenv('apiKey'),
        'scriptInfo': scriptInfo
    }
    body = json.dumps(data)
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=body, verify=False)
    print(response)
    print(response.json())


def saveFileInfo(scriptInfo: dict, tableFileName: str, description: str = ''):
    """Create or update a Datawrapper graph and store information about the graph in a database.

    :param scriptInfo: dictionary with information about the script
    :param tableFileName: name of the CSV file
    :param description: optional description of this File (markdown syntax allowed)
    """
    url = os.getenv('apiURLFile')
    data = {
        'apiKey': os.getenv('apiKey'),
        'scriptInfo': scriptInfo,
        'tableFileName': tableFileName,
        'description': description
    }
    body = json.dumps(data)
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=body, verify=False)
    print(response)
    print(response.json())
