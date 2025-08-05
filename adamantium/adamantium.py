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
from string import punctuation
punctuation = punctuation.replace(']', '')
from unidecode import unidecode
        
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
                            value = flat.iloc[0][flat_col]
                            if isinstance(value, (list, dict)):
                                value = str(value)
                            df.at[i, flat_col] = value
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

def get_folio_token():
    tenant_id = input('Enter your Folio tenant ID: ')    
    okapi_url='https://okapi-duquesne.folio.ebsco.com'
    headers = {
        'Content-Type': 'application/json',
        'x-okapi-tenant': tenant_id
    }
    payload = {
        'username': 'gumberg-api',
        'password': 'GumbergLibraryAPIOnly10/24/2024'
    }
    response = requests.post(f'{okapi_url}/authn/login', json=payload, headers=headers)
    if response.status_code == 201:
        token = response.headers.get('x-okapi-token')
        print("Access token retrieved successfully.")
        return token, tenant_id
    else:
        print(f"Login failed: {response.status_code} - {response.text}")
        return None

def get_folio_token():
    tenant_id = 'fs00001138'  
    okapi_url='https://okapi-duquesne.folio.ebsco.com'
    headers = {
        'Content-Type': 'application/json',
        'x-okapi-tenant': tenant_id
    }
    payload = {
        'username': 'api-users',
        'password': 'Api@UsersTest1'
    }
    response = requests.post(f'{okapi_url}/authn/login', json=payload, headers=headers)
    if response.status_code == 201:
        token = response.headers.get('x-okapi-token')
        print("Access token retrieved successfully.")
        return token, tenant_id
    else:
        print(f"Login failed: {response.status_code} - {response.text}")
        return None

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

def subject_field_adder(call_dict):
    gumberg_dict = {}
    subject_dict = {}
    subfield_dict = {}
    gumberg_subject_dict = {
    'General Works': ['AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI', 'AJ', 'AK', 'AL', 'AM', 'AN', 'AO', 'AP', 'AQ', 'AR', 'AS', 'AT', 'AU', 'AV', 'AW', 'AX', 'AY', 'AZ', 'A'],
    'Philosophy (1)': ['BA', 'BB', 'BC', 'BD', 'B'],
    'Philosophy (2)': ['BH', 'BI', 'BJ'],
    'Psychology': ['BF'],
    'Religion, Theology': ['BL', 'BM', 'BN', 'BO', 'BP', 'BQ', 'BR', 'BS', 'BT', 'BU', 'BV', 'BW', 'BX'],
    'History': ['C', 'CA', 'CB', 'CC', 'CD', 'CE', 'CF', 'CG', 'CH', 'CI', 'CJ', 'CK', 'CL', 'CM', 'CN', 
                'CO', 'CP', 'CQ', 'CR', 'CS', 'CT', 'CU', 'CV', 'CW', 'CX', 'CY', 'CZ', 'D', 'DA', 'DB', 
                'DC', 'DD', 'DE', 'DF', 'DG', 'DH', 'DI', 'DJ', 'DK', 'DL', 'DM', 'DN', 'DO', 'DP', 'DQ', 
                'DR', 'DS', 'DT', 'DU', 'DV', 'DW', 'DX', 'DY', 'DZ', 'E', 'EA', 'EB', 'EC', 'ED', 'EE', 'EF', 
                'EG', 'EH', 'EI', 'EJ', 'EK', 'EL', 'EM', 'EN', 'EO', 'EP', 'EQ', 'ER', 'ES', 'ET', 'EU', 'EV', 
                'EW', 'EX', 'EY', 'EZ', 'F', 'FA'],
    'Geography': ['GA', 'GB', 'GC', 'G'],
    'Environmental Sciences': ['GE'],
    'Anthropology': ['GF', 'GG', 'GH', 'GI', 'GJ', 'GK', 'GL', 'GM', 'GN', 'GO', 'GP', 'GQ', 'GR', 'GS', 'GT'],
    'Recreation, Leisure': ['GV'],
    'Social Sciences (General)': ['H', 'HB', 'HC', 'HD', 'HE', 'HF', 'HG', 'HH', 'HI', 'HJ', 'HK', 'HL', 'HM', 'HN', 'HO', 'HP', 'HQ', 'HR', 'HS', 'HT', 'HU', 'HV', 'HW', 'HX', 'HY', 'HZ'],
    'Statistics': ['HA'],
    'Economics, Finance': ['HB', 'HC', 'HD', 'HE', 'HF', 'HG', 'HH', 'HI', 'HJ'],
    'Sociology': ['HM', 'HN', 'HO', 'HP', 'HQ', 'HR', 'HS', 'HT', 'HU', 'HV'],
    'Socialism, Communism, Anarchism': ['HX'],
    'Political Science': ['J', 'JA', 'JB', 'JC', 'JD', 'JE', 'JF', 'JG', 'JH', 'JI', 'JJ', 'JK', 'JL', 'JM', 'JN', 'JO', 'JP', 'JQ', 'JR', 'JS', 'JT', 'JU', 'JV', 'JW', 'JX', 'JY', 'JZ'],
    'Law': ['K', 'KA', 'KB', 'KC', 'KD', 'KE', 'KF', 'KG', 'KH', 'KI', 'KJ', 'KK', 'KL', 'KM', 'KN', 'KO', 'KP', 'KQ', 'KR', 'KS', 'KT', 'KU', 'KV', 'KW', 'KX', 'KY', 'KZ'],
    'Education': ['L','LA','LB','LC','LD','LE','LF','LG','LH','LI','LJ','LK','LL','LM','LN','LO','LP','LQ','LR','LS','LT','LU','LV','LW','LX','LY','LZ'],
    'Music': ['M','MA','MB','MC','MD','ME','MF','MG','MH','MI','MJ','MK','ML','MM','MN','MO','MP','MQ','MR','MS','MT','MU','MV','MW','MX','MY','MZ'],
    'Fine Arts': ['N','NA','NB','NC','ND','NE','NF','NG','NH','NI','NJ','NK','NL','NM','NN','NO','NP','NQ','NR','NS','NT','NU','NV','NW','NX','NY','NZ'],
    'Language, Literature': ['P','PA','PB','PC','PD','PE','PF','PG','PH','PI','PJ','PK','PL','PM','PN','PO','PP','PQ','PR','PS','PT'],
    'Childrens Literature': ['PZ'],
    'Science (General)': ['Q', 'QF', 'QG', 'QI', 'QJ', 'QN', 'QO', 'QQ', 'QS', 'QT', 'QU', 'QV', 'QW', 'QX', 'QY', 'QZ'],
    'Mathematics': ['QA'],
    'Astronomy': ['QB'],
    'Physics': ['QC'],
    'Chemistry': ['QD'],
    'Geology': ['QE'],
    'Natural History, Biology': ['QH'],
    'Botany': ['QK'],
    'Zoology': ['QL'],
    'Human Anatomy': ['QM'],
    'Physiology': ['QP'],
    'Microbiology': ['QR'],
    'Medicine (1)': ['R','RA','RB'],
    'Medicine (2)': ['RD','RE','RF','RG','RH','RI','RJ','RK','RL','RM'],
    'Medicine (3)': ['RV', 'RW', 'RX', 'RY', 'RZ'],
    'Other Medicine':['RN', 'RO', 'RP', 'RQ', 'RR', 'RT', 'RU'],
    'Neurosciences, Biological Psychiatry': ['RC'],
    'Pharmacy, Materia Medica': ['RS'],
    'Nursing': ['RT'],
    'Agriculture': ['S', 'SA', 'SB', 'SC', 'SD', 'SE', 'SF', 'SG', 'SH', 'SI', 'SJ', 'SK', 'SL', 'SM', 'SN', 'SO', 'SP', 'SQ', 'SR', 'SS', 'ST', 'SU', 'SV', 'SW', 'SX', 'SY', 'SZ'],
    'Technology': ['T', 'TA', 'TB', 'TC', 'TD', 'TE', 'TF', 'TG', 'TH', 'TI', 'TJ', 'TK', 'TL', 'TM', 'TN', 'TO', 'TP', 'TQ', 'TR', 'TS', 'TT', 'TU', 'TV', 'TW', 'TX', 'TY', 'TZ'],
    'Military and Naval Science': ['U', 'UA', 'UB', 'UC', 'UD', 'UE', 'UF', 'UG', 'UH', 'UI', 'UJ', 'UK', 'UL', 'UM', 'UN', 'UO', 'UP', 'UQ', 'UR', 'US', 'UT', 'UU', 'UV', 'UW', 'UX',
                                   'UY', 'UZ', 'V', 'VA', 'VB', 'VC', 'VD', 'VE', 'VF', 'VG', 'VH', 'VI', 'VJ', 'VK', 'VL', 'VM', 'VN', 'VO', 'VP', 'VQ', 'VR', 'VS', 'VT', 'VU', 'VV',
                                   'VW', 'VX', 'VY', 'VZ'],
    'Bibliography, Library Science, Books, and Book Industry': ['Z', 'ZB', 'ZC', 'ZD', 'ZE', 'ZF', 'ZG', 'ZH', 'ZI', 'ZJ', 'ZK', 'ZL', 'ZM', 'ZN', 'ZO', 'ZP',
                                                                'ZQ', 'ZR', 'ZS', 'ZT', 'ZU', 'ZV', 'ZW', 'ZX', 'ZY', 'ZZ'],
    'Information Resources (General)': ['ZA']
}
    invalid_dict = {
    'LITERATURE KT': 'Childrens Paraphernalia',
    'CD': 'CD',
    'PUPPET': 'Puppet',
    'MANIP': 'Realia',
    'DVD': 'DVD',
    'PRINT': 'Journal',
    'LOT': 'Library of Things',
    'CIRC': 'Circulation Desk',
    'APPLE CHARGER':'Circulation Desk',
    'PPID': 'PPID',
    'LANGUAGE ARTS': 'Language Arts',
    'MEDIA RESERVE': 'Media Reserve',
    'MUSIC KT': 'Misc. Music',
    'SCIENCE KT': 'Misc. Science',
    'SOCIAL STUDIES KT': 'Misc. Social Studies',
    'SGA RESERVE ROOM': 'SGA Reserve Room',
    'CIRC':'Circulation Desk',
    'DRY ERASE MARKERS':'Circulation Desk',
    'KEY':'Circulation Desk',
    'Key':'Circulation Desk',
    'Key for Archives':'Circulation Desk',
    'Key For Archives':'Circulation Desk',
    'SOUND ISOLATION ROOM':'Circulation Desk',
    'Sound Isolation Room':'Circulation Desk',
    'SGA RESERVE':'Personal Course Reserves',
    'MEDICAL MODEL':'Medical Model',
    'RESERVE ROOM PERSONAL':'Personal Course Reserves'
    }

    LoC_dict = {
    'A': 'General Works',
    'B': 'Philosophy, Psychology, Religion',
    'BA': 'Philosophy (General)',
    'BC': 'Logic',
    'BD': 'Speculative Philosophy',
    'BF': 'Psychology',
    'BH': 'Aesthetics',
    'BJ': 'Ethics',
    'BL': 'Religions. Mythology. Rationalism',
    'BM': 'Judaism',
    'BP': 'Islam. Bahaism. Theosophy, etc.',
    'BQ': 'Buddhism',
    'BR': 'Christianity',
    'BS': 'The Bible',
    'BT': 'Doctrinal Theology',
    'BV': 'Practical Theology',
    'BX': 'Christian Denominations',
    'C': 'Auxiliary Sciences of History',
    'CA': 'Archaeology',
    'CB': 'History of Civilization',
    'CC': 'Archaeology (General)',
    'CD': 'Diplomatics. Archives. Seals',
    'CE': 'Technical Chronology. Calendar',
    'CJ': 'Numismatics',
    'CN': 'Inscriptions. Epigraphy',
    'CR': 'Heraldry',
    'CS': 'Genealogy',
    'CT': 'Biography',
    'D': 'History (General) and History of Europe',
    'DA': 'Great Britain',
    'DAW': 'Central Europe',
    'DB': 'Austria - Liechtenstein - Hungary - Czechoslovakia',
    'DC': 'France - Andorra - Monaco',
    'DD': 'Germany',
    'DE': 'Greco-Roman World',
    'DF': 'Greece',
    'DG': 'Italy - Malta',
    'DH': 'Low Countries - Benelux Countries',
    'DJ': 'Netherlands (Holland)',
    'DJK': 'Eastern Europe (General)',
    'DK': 'Russia. Soviet Union. Former Soviet Republics - Poland',
    'DL': 'Northern Europe. Scandinavia',
    'DP': 'Spain - Portugal',
    'DQ': 'Switzerland',
    'DR': 'Balkan Peninsula',
    'DS': 'Asia',
    'DT': 'Africa',
    'DU': 'Oceania (South Seas)',
    'DV': 'Eastern Hemisphere',
    'DX': 'Romanies',
    'E': 'History: America',
    'F': 'History: America',
    'F1': 'United States Local History',
    'F2': 'Great Basin. NV, UT',
    'F3': 'Great Lakes Area. MI, MN, NY, OH, PA, WI',
    'F4': 'Middle Atlantic States. DC, DE, MD, NJ, NY, PA',
    'F5': 'New England. CT, ME, MA, NH, RI, VT',
    'F7': 'Southeastern States. AL, AR, FL, GA, KY, LA, MS, NC, SC, TN, VA, WV',
    'F8': 'South Central States. AR, LA, OK, TX',
    'F9': 'Western States. CO, ID, MT, NE, NV, ND, SD, UT, WY',
    'G': 'Geography, Anthropology, Recreation',
    'GA': 'Mathematical Geography. Cartography',
    'GB': 'Physical Geography',
    'GC': 'Oceanography',
    'GE': 'Environmental Sciences',
    'GF': 'Human Ecology. Anthropogeography',
    'GN': 'Anthropology',
    'GR': 'Folklore',
    'GT': 'Manners and Customs (General)',
    'GV': 'Recreation. Leisure',
    'H': 'Social Sciences',
    'HA': 'Statistics',
    'HB': 'Economic Theory. Demography',
    'HC': 'Economic History and Conditions',
    'HD': 'Industries. Land Use. Labor',
    'HE': 'Transportation and Communications',
    'HF': 'Commerce',
    'HG': 'Finance',
    'HJ': 'Public Finance',
    'HM': 'Sociology (General)',
    'HN': 'Social History and Conditions. Social Problems. Social Reform',
    'HQ': 'The Family. Marriage. Women',
    'HS': 'Societies: Secret, Benevolent, etc.',
    'HT': 'Communities. Classes. Races',
    'HV': 'Social Pathology. Social and Public Welfare',
    'HX': 'Socialism. Communism. Anarchism',
    'J': 'Political Science',
    'JA': 'Political Science (General)',
    'JC': 'Political Theory',
    'JF': 'Political Institutions and Public Administration',
    'JJ': 'Political Institutions and Public Administration (North America)',
    'JK': 'Political Institutions and Public Administration (United States)',
    'JL': 'Political Institutions and Public Administration (Canada, Latin America, etc.)',
    'JN': 'Political Institutions and Public Administration (Europe)',
    'JQ': 'Political Institutions and Public Administration (Asia, Africa, Australia, Pacific Area, etc.)',
    'JV': 'Colonies and Colonization. Emigration and Immigration. International Migration',
    'JZ': 'International Relations',
    'K': 'Law',
    'KB': 'Religious Law in General. Comparative Religious Law. Jurisprudence',
    'KBM': 'Jewish Law',
    'KBP': 'Islamic Law',
    'KBR': 'History of Canon Law',
    'KBU': 'Law of the Roman Catholic Church. The Holy See',
    'KD': 'United Kingdom and Ireland',
    'KE': 'Canada',
    'KF': 'United States',
    'KG': 'Latin America (General)',
    'KH': 'South America (General)',
    'KJ': 'Europe (General)',
    'KK': 'Scandinavia',
    'KL': 'Asia and Eurasia, Africa, Pacific Area, and Antarctica',
    'KM': 'Eastern Hemisphere',
    'KN': 'Africa',
    'KP': 'Australia. New Zealand. Pacific Ocean Islands',
    'KZ': 'Law of Nations',
    'L': 'Education',
    'LA': 'History of Education',
    'LB': 'Theory and Practice of Education',
    'LC': 'Special Aspects of Education',
    'LD': 'Individual Institutions - United States',
    'LE': 'Individual Institutions - America (except United States)',
    'LF': 'Individual Institutions - Europe',
    'LG': 'Individual Institutions - Asia, Africa, Indian Ocean Islands, Australia, New Zealand, Pacific Islands',
    'LH': 'College and School Magazines and Papers',
    'LJ': 'Student Fraternities and Societies, United States',
    'LT': 'Textbooks',
    'M': 'Music',
    'ML': 'Literature on Music',
    'MT': 'Musical Instruction and Study',
    'N': 'Fine Arts',
    'NA': 'Architecture',
    'NB': 'Sculpture',
    'NC': 'Drawing. Design. Illustration',
    'ND': 'Painting',
    'NE': 'Print Media',
    'NK': 'Decorative Arts',
    'NX': 'Arts in General',
    'P': 'Language and Literature',
    'PA': 'Classical Languages and Literature',
    'PB': 'Modern Languages. Celtic Languages',
    'PC': 'Romance Languages',
    'PD': 'Germanic Languages. Scandinavian Languages',
    'PE': 'English Language',
    'PF': 'West Germanic Languages',
    'PG': 'Slavic Languages and Literatures. Baltic Languages. Albanian Language',
    'PH': 'Uralic Languages. Basque Language',
    'PJ': 'Oriental Languages and Literatures',
    'PK': 'Indo-Iranian Languages and Literatures',
    'PL': 'Languages and Literatures of Eastern Asia, Africa, Oceania',
    'PM': 'Hyperborean, Native American, and Artificial Languages',
    'PN': 'Literature (General)',
    'PQ': 'French Literature - Italian Literature - Spanish Literature - Portuguese Literature',
    'PR': 'English Literature',
    'PS': 'American Literature',
    'PT': 'German Literature - Dutch Literature - Flemish Literature Since 1830 - Afrikaans Literature - Scandinavian Literature - Old Norse Literature: Old Icelandic and Old Norwegian - Modern Icelandic Literature - Faroese Literature - Danish Literature - Norwegian Literature - Swedish Literature',
    'PZ': 'Fiction and Juvenile Belles Lettres',
    'O': 'Science (General)',
    'A': 'General Works',
    'AB': 'General Collections',
    'AC': 'Collections. Series. Collected Works',
    'AE': 'Encyclopedias',
    'AG': 'Dictionaries and Other General Reference Works',
    'AI': 'Indexes',
    'AM': 'Museums. Collectors and Collecting',
    'AN': 'Newspapers',
    'AP': 'Periodicals',
    'AS': 'Academies and Learned Societies',
    'AY': 'Yearbooks. Almanacs. Directories',
    'Q': 'Science',
    'QA': 'Mathematics',
    'QB': 'Astronomy',
    'QC': 'Physics',
    'QD': 'Chemistry',
    'QE': 'Geology',
    'QH': 'Natural History',
    'QK': 'Botany',
    'QL': 'Zoology',
    'QM': 'Human Anatomy',
    'QP': 'Physiology',
    'QR': 'Microbiology',
    'R': 'Medicine',
    'RA': 'Public Aspects of Medicine',
    'RB': 'Pathology',
    'RC': 'Internal Medicine',
    'RD': 'Surgery',
    'RE': 'Ophthalmology',
    'RF': 'Otorhinolaryngology',
    'RG': 'Gynecology and Obstetrics',
    'RJ': 'Pediatrics',
    'RK': 'Dentistry',
    'RL': 'Dermatology',
    'RM': 'Therapeutics. Pharmacology',
    'RS': 'Pharmacy and Materia Medica',
    'RT': 'Nursing',
    'RV': 'Botanic, Thomsonian, and Eclectic Medicine',
    'RX': 'Homeopathy',
    'RZ': 'Other Systems of Medicine',
    'S': 'Agriculture',
    'SB': 'Plant Culture',
    'SD': 'Forestry',
    'SF': 'Animal Culture',
    'SH': 'Aquaculture. Fisheries. Angling',
    'SK': 'Hunting Sports',
    'T': 'Technology',
    'TA': 'Engineering (General). Civil Engineering',
    'TC': 'Hydraulic Engineering. Ocean Engineering',
    'TD': 'Environmental Technology. Sanitary Engineering',
    'TE': 'Highway Engineering. Roads and Pavements',
    'TF': 'Railroad Engineering and Operation',
    'TG': 'Bridge Engineering',
    'TH': 'Building Construction',
    'TJ': 'Mechanical Engineering and Machinery',
    'TK': 'Electrical Engineering. Electronics. Nuclear Engineering',
    'TL': 'Motor Vehicles. Aeronautics. Astronautics',
    'TN': 'Mining Engineering. Metallurgy',
    'TP': 'Chemical Technology',
    'TR': 'Photography',
    'TS': 'Manufactures',
    'TT': 'Handicrafts. Arts and Crafts',
    'TX': 'Home Economics',
    'U': 'Military Science',
    'UA': 'Armies: Organization, Distribution, Military Situation',
    'UB': 'Military Administration',
    'UC': 'Maintenance and Transportation',
    'UD': 'Infantry',
    'UE': 'Cavalry. Armor',
    'UF': 'Artillery. Air Forces. Troops in General',
    'UG': 'Military Engineering. Air Forces',
    'UH': 'Other Services',
    'V': 'Naval Science',
    'VA': 'Navies: Organization, Distribution, Naval Situation',
    'VB': 'Naval Administration',
    'VC': 'Naval Maintenance',
    'VD': 'Naval Seamen',
    'VE': 'Marine Engineering. Naval Engineering',
    'VF': 'Naval Ordnance',
    'VG': 'Minor Services of Navies',
    'VK': 'Navigation. Merchant Marine',
    'VM': 'Naval Architecture. Shipbuilding. Marine Engineering',
    'Z': 'Bibliography. Library Science'
}
    for key, value in call_dict.items():
        if key == '':
            subject = key
            call = key
        else:
            subject = key[0]
            call = key[0:2]
        if value == 'Valid':
            if call[1].isnumeric():
                call = call[0]
            else:
                pass
            for item in gumberg_subject_dict.values():
                if call in item:
                    subber = list(gumberg_subject_dict.keys())[list(gumberg_subject_dict.values()).index(item)]
                    gumberg_dict[key] = str(subber)
            for item in LoC_dict.keys():
                if subject == item:
                    subject_dict[key] = LoC_dict[item]
                if call == item:
                    subfield_dict[key] = LoC_dict[item]
        if value == 'Invalid':
            key_check = 0
            for k in invalid_dict.keys():
                if k in key:
                    key_check = 1
                    gumberg_dict[key] = invalid_dict[k]
            if key_check < 1:
                for item in gumberg_subject_dict.values():
                    if call in item:
                        subber = list(gumberg_subject_dict.keys())[list(gumberg_subject_dict.values()).index(item)]
                        gumberg_dict[key] = str(subber)
                for item in LoC_dict.keys():
                    if subject == item:
                        subject_dict[key] = LoC_dict[item]
                    if call == item:
                        subfield_dict[key] = LoC_dict[item]

    return gumberg_dict, subject_dict, subfield_dict

def check_classification_segments(call_numbers):
    results = {}
    for call_number in call_numbers:
        #Remove premature whitespace:
        if len(call_number) > 2:
            if call_number[2] == ' ':
                call = list(call_number)
                call[2] = ''
                call = ''.join(call)
            else:
                call = call_number
        else:
            call = call_number
        # Extract the classification segment (first part before any spaces)
        if call == '':
            classification_segment = call
        else:
            classification_segment = call.split()[0]
        results[call_number] = is_classification_segment(classification_segment)
    return results

def list_functions(module):
    return [name for name, obj in inspect.getmembers(module, inspect.isfunction)
            if obj.__module__ == module.__name__]

def show_function_code(module, func_name):
    try:
        func = getattr(module, func_name)
        if inspect.isfunction(func):
            print(f"\nSource code for '{func_name}':\n")
            print(inspect.getsource(func))
        else:
            print(f"'{func_name}' is not a function.")
    except Exception as e:
        print(f"Error retrieving function '{func_name}': {e}")

def module_help(module):
    funcs = list_functions(module)
    if not funcs:
        print(f"No functions found in {module.__name__}")
        return

    print(f"Functions in {module.__name__}:\n")
    for i, f in enumerate(funcs, 1):
        print(f"{i}. {f}")

    while True:
        choice = input("\nEnter a number to see the function code (or leave blank to exit): ").strip()
        if choice == "":
            break
        if not choice.isdigit():
            print("Please enter a valid number.")
            continue

        index = int(choice)
        if 1 <= index <= len(funcs):
            show_function_code(module, funcs[index - 1])
        else:
            print(f"Please choose a number between 1 and {len(funcs)}.")

def concat_df_dir(directory):
    
    df_list = []
    for file in os.listdir(directory):
        extension = file.split('.')[-1]
        if extension == 'csv':
            df = pd.read_csv(rf'{directory}\{file}', encoding = 'utf-8-sig')
        elif extension == 'xlsx':
            df = pd.read_excel(rf'{directory}\{file}', engine='openpyxl')
        else:
            print(f'{file} was not .csv or .xlsx - skipping')
        df_list.append(df)
    try:
        master = pd.concat(df_list)
    except Exception as e:
        print(f"Concat failed: {e}")
    
    return master

def title_fixer(unfixed_titles):
    fixed_titles = []
    for title in unfixed_titles:
        if title:
            title = str(title)
            title = title.replace('&', 'and')
            title = title.replace(',', '')
            title = title.rstrip(punctuation)
            title = unidecode(title)
            title = title.split()
            new_title = []
            for word in title:
                if word.isupper():
                    new_title.append(word)
                else:
                    word = string.capwords(word)
                    new_title.append(word)
            new_title = ' '.join(new_title)
            if new_title == 'Nan':
                new_title = ''
            fixed_titles.append(new_title)
        else:
            fixed_titles.append('')
    return fixed_titles

