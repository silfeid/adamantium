# -*- coding: utf-8 -*-
"""
Created on Thu Jun 19 10:13:06 2025

@author: brodea
"""

import pandas as pd
import os
import ast
import requests
import time
import regex as re
from datetime import datetime
import inspect

def show_function_source(func):
    """Print the source code of any function defined in the script."""
    try:
        source = inspect.getsource(func)
        print(source)
    except (TypeError, OSError) as e:
        print(f"Could not retrieve source: {e}")

def show_guidance():
    funcs = {1:'junkdrawer', 2:'saver', 3:'fetcher', 4:'recursive_flatten', 5:'folio_api_call', 6:'clean_titles', 7:'libinsight_api_call', 8:'remove_carriage_returns', 9:'fetch_libinsight_token'}
    for key, value in funcs.items():
        print(key, value)
    guidance = int(input('\nEnter the number of the function that you wish to examine: '))
    try:
        if guidance in funcs.keys():
            print(guidance)
            if guidance == 1:
                print('The function "junkdrawer" saves files to Documents\Analytics\Junkdrawer and requires a data frame as a variable.')
            elif guidance == 2:
                print('The function "saver" saves files to a directory of your choosing and requires a data frame and a directory as variables; the first variable is the data frame, the second is the directory as a string.  If you do not input a directory, you will be prompted for one.')
            elif guidance == 3:
                print('The function "fetcher" gets the name of the most recently created file in a directory; it takes no variables, you will instead manually input the directory path.')
            elif guidance == 4:
                print('The function "recursive flatten" un-nests json data where cells in the df are populated by lists (usually even after json_normalize etc.).  It takes a df as its single variable.')
            elif guidance == 5:
                print('The function "folio_api_call" executes an API call to Folio (duh).  You need to feed it your endpoint (no base url needed), tenant_id, and token, in that order.')
            elif guidance == 6:
                print('The function "clean_titles" does a nice title-case cleanup job on titles, way better than Python\'s built-in version.  It takes a string as its input; probably mostly you will want to use it on a df[column] with .apply()')
            elif guidance == 7:
                print('The function "libinsight_api_call" executes an API call to LibInsight (duh).  You need to feed it a bunch of crap - that one is not ready yet. ')
            elif guidance == 8:
                print('The function "remove_carriage_returns" does what it says to an entire df - the only input variable needed.')
            elif guidance == 9:
                print('The function "fetch_libinsight_token" does what its name implies; you need to feed it two values after running it: client_id and client_secret, in that order.  Get them from the LibInsight website (Widgets and APIs > Manage API Authentication); then copy-paste them when prompted after running the function.')
    except:
        print('Summat is broken, sir')
        
def junkdrawer(df, label=None):
    if not label:
        label = input("Enter a label for the DataFrame or press enter to use default (df): ").strip()
        if not label:
            label = "df"
    try:
        df.to_csv('C:/Users/brodea/Documents/Analytics/Junkdrawer/'+label+'.csv', index=False, encoding = 'utf-8-sig')
        print(f'Data frame saved to junk drawer as {label}.csv')
    except AttributeError:
        print('Variable is not a DataFrame and cannot be exported as .csv file.')
    except PermissionError:        
        df.to_csv('C:/Users/brodea/Documents/Analytics/Junkdrawer/'+label+'1'+'.csv', index=False, encoding = 'utf-8-sig')
        print(f'Data frame saved to junk drawer as {label}.csv')
        
def saver(df, directory=None, label=None):
    if not label:
        label = input("Enter a label for the DataFrame or press enter to use default (df): ").strip()
        if not label:
            label = "df"
    if directory is None:
        directory = input('Enter the directory to which you wish to save, starting with "C:\" : ')
    try:
        df.to_csv(rf'{directory}\{label}.csv', index=False, encoding = 'utf-8-sig')
        print(f'Data frame saved to {directory} as {label}.csv')
    except AttributeError:
        print('Variable is not a DataFrame and cannot be exported as .csv file.')
    except PermissionError:        
        df.to_csv(rf'{directory}\{label}-1.csv', index=False, encoding = 'utf-8-sig')
        print(f'Data frame saved to junk drawer as {label}-1.csv')
        
def fetcher(multipass =None, directory=None, extension=None):
    
    if directory is None:
        directory = (input('Enter the filepath of your data directory starting with Box (e.g., Box\Annual Report Procedures\Dashboard\Data\Raw Data): '))
    if multipass is None:
        multipass = input('Enter your multipass username: ')
    if extension is None:
        extension = input('Extension of data files (.csv or .xlsx): ')
    
    directory = rf'C:\Users\{multipass}\{directory}'
    download_dict = {}
    
    for filename in os.listdir(directory):
        if filename.endswith(extension):
            download_dict[filename] = os.path.getctime(directory+"/"+filename)
    
    #Get the value of the most newly created file in our dictionary:
    newest_data = max(download_dict.values())
    
    for key, value in download_dict.items():
        if value == newest_data:
            newest_filename = key
            
    filepath = rf'{directory}\{newest_filename}'        
    
    print(rf'The most recent file in that directory is named: {newest_filename}')

    return filepath, multipass, newest_filename

def load_most_recent_df(multipass =None, directory=None, extension=None):
    
    if directory is None:
        directory = (input('Enter the filepath of your data directory starting with Box (e.g., Box\Annual Report Procedures\Dashboard\Data\Raw Data): '))
    if multipass is None:
        multipass = input('Enter your multipass username: ')
    if extension is None:
        extension = input('Extension of data files (.csv or .xlsx): ')
    
    directory = rf'C:\Users\{multipass}\{directory}'
    download_dict = {}
    
    for filename in os.listdir(directory):
        if filename.endswith(extension):
            download_dict[filename] = os.path.getctime(directory+"/"+filename)
    
    #Get the value of the most newly created file in our dictionary:
    newest_data = max(download_dict.values())
    
    for key, value in download_dict.items():
        if value == newest_data:
            newest_filename = key
            
    filepath = rf'{directory}\{newest_filename}'        
    
    print(rf'The most recent file in that directory is named: {newest_filename}')
    
    while True:
        ans = input('Do you wish to load that file (Y/N)?')
        if ans.lower() in ['y', 'n']:
            if ans.lower() == 'y':
                if extension == '.csv':
                    print('Loading .csv file...')
                    df = pd.read_csv(filepath)
                elif extension == '.xlsx':
                    print('Loading Excel file...')
                    df = pd.read_excel(filepath, engine='openpyxl')
                break
            elif ans.lower() == 'n':
                print('Understood. The function will return the data frame variable as a None value. All other expected variables (filepath, multipass, newest_filename) will be returned as normal.')
                df = None
                break

    return filepath, multipass, newest_filename, df

def recursive_flatten(df):
    # First pass: convert stringified lists/dicts
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) and (x.strip().startswith('[') or x.strip().startswith('{')) else x
        )
    
    keep_going = True
    while keep_going:
        keep_going = False
        for col in df.columns:
            col_data = df[col].dropna()

            if col_data.apply(lambda x: isinstance(x, dict)).any():
                # Flatten dicts at the row level
                for i, row in df.iterrows():
                    val = row[col]
                    if isinstance(val, dict):
                        flat = pd.json_normalize(val).add_prefix(f"{col}.")
                        for flat_col in flat.columns:
                            df.at[i, flat_col] = flat.iloc[0][flat_col]
                        df.at[i, col] = None
                        keep_going = True
                continue

            if col_data.apply(lambda x: isinstance(x, list) and all(isinstance(i, dict) for i in x)).any():
                # Flatten list of dicts at row level
                for i, row in df.iterrows():
                    val = row[col]
                    if isinstance(val, list) and all(isinstance(i, dict) for i in val):
                        combined = {}
                        for d in val:
                            combined.update(d)
                        for k, v in combined.items():
                            df.at[i, f"{col}.{k}"] = v
                        df.at[i, col] = None
                        keep_going = True
                continue

            if col_data.apply(lambda x: isinstance(x, list)).any():
                # Turn list of scalars into strings
                df[col] = df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
                keep_going = True

    # Optionally drop empty columns if they resulted from flattening
    df = df.dropna(axis=1, how='all')
    return df

def folio_api_call(endpoint, tenant_id, token):
    url = f"https://okapi-duquesne.folio.ebsco.com/{endpoint}"
    # Headers for authentication
    #Access token
    headers = {
        "X-Okapi-Tenant": tenant_id,
        "X-Okapi-Token": token,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    params = {
        'limit': 10000000,  # Number of items per page
        'offset': 0    # Initial offset
    }
    
    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            response.data = response.json()
            if response.data['totalRecords'] > 0:
                data = next(iter(response.data.values()))
                # Check if there are more records to fetch
                if params['offset'] + params['limit'] < response.data['totalRecords']:
                    params['offset'] += params['limit']
                    time.sleep(1)  # Add a delay of 1 second before making the next request
                else:
                    # All records fetched
                    break
            else:
                # No more records
                break
            
        elif response.status_code == 401:
            print('Token expired')
            token = input('new token please: ')
            return folio_api_call(endpoint, tenant_id, token)
            
        elif response.status_code != 401:
            print("Failed to retrieve data. Error code:", response.status_code)
            print(response.text)  # Print response content for debugging
            break
    if data:
        data = recursive_flatten(pd.DataFrame(data))   
    return data

def libinsight_api_call(token, dataset_id=None):
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json',
    }
    
    params = {'page':1}
    all_items = []
    start_date, end_date = get_date_range()
    if dataset_id is None:
        dataset_id = input('Enter the dataset ID number: ')
    api_url = f'https://duq.libinsight.com/v1.0/custom-dataset/{dataset_id}/data-grid?from={start_date}&to={end_date}'
    
    while True:
        response = requests.get(api_url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            data = data['payload']
            q_data = data['records']
            all_items.extend(q_data)
            count = params['page']
            print(f'Page {count} of data added')
        else:
            print(f"Error {response.status_code}: {response.text}")
        if data['displayed_page'] < data['total_pages']:
            params['page'] += 1
        else:
            break
    df = pd.DataFrame(all_items)
    return df, start_date, end_date

def get_date_range():
    while True:
        start_date_str = input("Enter a start date (YYYY-MM-DD): ")
        try:
            valid_start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            break
        except ValueError:
            print("Invalid format. Please enter a date in YYYY-MM-DD format.")
    while True:
        end_date_str = input("Enter an end date (YYYY-MM-DD): ")
        try:
            valid_end_date = datetime.strptime(end_date_str, "%Y-%m-%d") 
            if valid_start_date < valid_end_date:
                break
            else:
                print('End date is prior or equal to start date.  Try again.')
        except ValueError:
            print("Invalid format. Please enter a date in YYYY-MM-DD format.")

    return start_date_str, end_date_str
            
def clean_titles(title):

    def smart_title_case(text):

        def smart_capitalize(word, is_start):

            def is_acronym(word):
                return len(word) > 1 and all(c.isupper() for c in word if c.isalpha())

            lowercase_words = {
                'a', 'an', 'and', 'as', 'at', 'but', 'by', 'for', 'from', 'in', 'into', 'underneath', 'below', 'behind',
                'through', 'with', 'within', 'without', 'inside', 'near', 'nor', 'of', 'off', 'on', 'onto', 'out', 'over',
                'per', 'since', 'than', 'the', 'to', 'up', 'upon', 'via', 'auf', 'aus', 'bei', 'durch', 'für', 'gegen',
                'hinter', 'im', 'innerhalb', 'ausserhalb', 'mit', 'nach', 'neben', 'ohne', 'seit', 'über', 'um', 'unter',
                'von', 'vor', 'während', 'wegen', 'zu', 'zur', 'zwischen', 'à', 'après', 'avant', 'avec', 'chez',
                'contre', 'dans', 'de', 'depuis', 'derrière', 'devant', 'durant', 'en', 'entre', 'jusqu’à', 'malgré',
                'par', 'parmi', 'pendant', 'pour', 'sans', 'selon', 'sous', 'sur', 'vers', 'voici', 'voilà', 'ante',
                'bajo', 'cabe', 'con', 'contra', 'desde', 'durante', 'hacia', 'hasta', 'mediante', 'para', 'por',
                'salvo', 'según', 'sin', 'so', 'sobre', 'tras', 'versus', 'vía', 'após', 'até', 'com', 'perante', 'sem',
                'sob', 'trás', 'ad', 'alla', 'alle', 'al', 'ai', 'col', 'contro', 'dal', 'dai', 'dalla', 'dalle',
                'di', 'del', 'della', 'dello', 'dei', 'delle', 'dentro', 'fuori', 'nel', 'nella', 'nei', 'nelle', 'senza',
                'sopra', 'sotto', 'su', 'sul', 'sulla', 'tra', 'fra', 'der', 'die', 'das', 'den', 'dem', 'des', 'ein',
                'eine', 'einen', 'einem', 'einer', 'eines', 'le', 'la', 'les', 'l’', 'un', 'une', 'des', 'du', 'de la',
                'de l’', 'el', 'los', 'las', 'unos', 'unas', 'lo', 'os', 'as', 'um', 'uma', 'uns', 'umas', 'il', 'lo',
                'i', 'gli', 'un', 'uno', 'una', 'un\'', 'verso', 'und', 'y', 'e', 'ed', 'aber', 'mais', 'pero', 'mas',
                'ma', 'doch', 'sino'
            }

            if is_acronym(word):
                return word
            if word.lower() in lowercase_words and not is_start:
                return word.lower()
            if "'" in word:
                parts = word.split("'")
                parts = [parts[0].capitalize()] + [p.lower() for p in parts[1:]]
                return "'".join(parts)
            return word.capitalize()

        text = re.sub(r'\s*:\s*', ': ', text.strip())
        tokens = re.findall(r"\p{L}[\p{L}\p{M}\p{N}'’-]*|\P{L}+", text, flags=re.UNICODE)
        result = []
        capitalize_next = True

        for token in tokens:
            if re.match(r"\p{L}", token):
                result.append(smart_capitalize(token, capitalize_next))
                capitalize_next = False
            else:
                result.append(token)
                if re.search(r'[:(\[{—–]\s*$', token):
                    capitalize_next = True

        return ''.join(result)

    # Cleanup
    title = title.strip('/').strip()
    title = title.rstrip('.').rstrip(',').rstrip(';')
    title = title.replace('  ', ' ')
    title = smart_title_case(title)

    return title

def remove_carriage_returns(df):
    for col in df.select_dtypes(include=['object', 'string']):
        df[col] = df[col].map(lambda x: re.sub(r'[\r\n]+', ' ', x) if isinstance(x, str) else x)
    return df

def fetch_libinsight_token(client_id=None, client_secret=None):
    if client_id is None:
        client_id = input('Enter your Client ID: ')
    if client_secret is None:
        client_secret = input('Enter your Client Secret: ')
    token_url = 'https://duq.libinsight.com/v1.0/oauth/token'
    response = requests.post(
    token_url,
    data={
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
)

    response.raise_for_status()
    token = response.json()['access_token']
    return token

