
# %%
import requests
from requests.exceptions import ConnectTimeout, ReadTimeout
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re
import pandas as pd
from time import sleep
from urllib.parse import urlparse, parse_qs
from tqdm import tqdm
from typing import Tuple, Optional, Callable
import logging

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 200)

# %%
def _request_util(url: str, timeout: Tuple[int, int]=(10, 30), max_retries: int=5):
    """
    Make HTTP GET request with retry logic and error handling.
    
    Args:
        url (str): The full URL to request
        timeout (tuple): Connection and read timeout values
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        requests.Response or None: Response object if successful, None if failed
        
    Raises:
        requests.exceptions.RequestException: For unrecoverable request errors
    """
    retry_strategy = Retry(
        total=max_retries,
        status_forcelist=[429, 500, 502, 503, 504],  
        backoff_factor=1,
        allowed_methods=["GET"] 
    )

    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()  
        return response
        
    except requests.exceptions.Timeout as e:
        logging.error(f"Request timed out for URL {url}: {e}")
        return None
        
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Connection error for URL {url}: {e}")
        return None
        
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error for URL {url}: {e}")
        return None
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for URL {url}: {e}")
        raise  
        
    finally:
        session.close() 

def _get_pages(url_path: str, base_url: str = "https://ilga.gov") -> pd.DataFrame:
    """
    Fetches and parses links from FTP at https://ilga.gov/ftp/.

    Args:
        url_path (str, optional): Path to append to the base_url.
        base_url (str): Base URL of the site.

    Returns:
        pd.DataFrame: DataFrame with columns 'label' and 'href' for each link found.
    """
    response = _request_util(url = f'{base_url}{url_path}')
    soup = BeautifulSoup(response.text, 'html.parser')
    pre_tag = soup.find('pre')
    url_links = []

    if pre_tag:
        links = pre_tag.find_all('a')

        for link in links:
            href = link.get('href')
            label = link.get_text()

            if label != "[To Parent Directory]":
                url_links.append({"label": label, "href": href})

    data = pd.json_normalize(url_links)
    data = data[data['label'] != 'aReadMe']

    return(data)

def _parse_filestring(s: str) -> Tuple[str, str, str]:
    """
    Parses a file string into three components:
    - Unique statute code (first 9 digits)
    - Statute outline level (A, F, K, HArt, etc.)
    - Section numbers

    Args:
        s (str): The file string to parse.

    Returns:
        tuple: (first_9_digits, letters, remaining_numbers)
    """
    first_9_digits = s[:9]
    letters_pattern = re.compile(r'(A|F|K|HArt.|HTit.|HPt.|HDiv.|Hprec.|HCh.)')
    letters_match = letters_pattern.search(s[9:])
    letters = ''
    if letters_match:
        letters = letters_match.group(0)
    letters_pos = s.find(letters, 9) if letters else -1
    remaining_numbers = ''
    if letters_pos != -1:
        start_pos = letters_pos + len(letters)
        end_pos = s.find('.html')
        remaining_numbers = s[start_pos:end_pos]

    return first_9_digits, letters, remaining_numbers

def build_ilcs_index(base_url:str="https://ilga.gov", root_path:str="/ftp/ILCS/") -> pd.DataFrame:
    """
    Crawl the ILGA site hierarchy and build a DataFrame
    indexing chapters, acts, and sections.
    
    Parameters:
        base_url (str): top-level domain URL.
        root_path (str): Root path to start crawling.

    Returns:
        pd.DataFrame: DataFrame containing chapter, act, and section information.
    """

    df_index = pd.DataFrame( {
        'chapter_name': pd.Series(dtype='object'),
        'chapter_url': pd.Series(dtype='object'),
        'act_name': pd.Series(dtype='object'),
        'act_url': pd.Series(dtype='object'),
        'section_file': pd.Series(dtype='object'),
        'section_url': pd.Series(dtype='object')
        })

    chapters_data = _get_pages(base_url=base_url, url_path=root_path)

    for chapter_url in chapters_data['href']:
        chapter_name = chapters_data.loc[
            chapters_data['href'] == chapter_url, 'label'
        ].values[0]

        acts_data = _get_pages(base_url=base_url, url_path=chapter_url)

        for act_url in acts_data['href']:
            act_name = acts_data.loc[
                acts_data['href'] == act_url, 'label'
            ].values[0]

            sections_data = _get_pages(base_url=base_url, url_path=act_url)
            sections_data = sections_data.rename(columns={
                'label': 'section_file',
                'href': 'section_url'
            })

            print(f'Processing Chapter: {chapter_url}, Act: {act_url}')

            sections_data['chapter_name'] = chapter_name
            sections_data['chapter_url'] = chapter_url
            sections_data['act_name'] = act_name
            sections_data['act_url'] = act_url

            df_index = pd.concat(
                [df_index, sections_data],
                ignore_index=True
            )

    parsed = df_index['section_file'].apply(_parse_filestring)
    df_index[['ilcs_index_number', 'ilcs_index_type', 'ilcs_index_ext']] = pd.DataFrame(parsed.tolist(), index=df_index.index)

    mapping = {
        'A': 'Chapter (A)',
        'F': 'Act (F)',
        'K': 'Section (K)',
        'HArt.': 'Article (HArt)',
        'HPt.': 'Part (HPt)',
        'HTit.': 'Title (HTit)',
        'HDiv.': 'Division (HDiv)',
        'HArt ': 'Article (HArtDiv)',
        'Hprec.': 'Preceding Section (HprecSec)',
        'HArt,': 'Article (HArt)',
        'HCh.': 'Chapter (HCh)',
        'HCh ': 'Chapter (HChArt)',
    }

    df_index['ilcs_index_type_label'] = df_index['ilcs_index_type'].map(mapping).fillna('Unknown')

    return df_index

def parse_act_page(url_path:str, base_url:str = "https://ilga.gov") -> pd.DataFrame:
    """
    Extracts Act text elements from ILGA HTML page using BeautifulSoup
    and organizes them into a pandas DataFrame.

    Args:
        url_path (str): the URL path that follows the base_url.
        base_url (str):  top-level domain URL.

    Returns:
        pandas.DataFrame: A DataFrame with columns for ILCS code, ILCS act title,
                        title description, cite, source, and short title.
    """

    response = _request_util(url = f'{base_url}{url_path}')
    soup = BeautifulSoup(response.text, 'html.parser')
    data = {}

    # Find the main div containing the text
    div = soup.find('div', align='justify')
    if div:
        full_text = div.get_text(separator='\n', strip=True)

        # Extract ILCS code
        ilcs_code_match = re.search(r'^\((.*?)\)', full_text)
        data['ilcs_code'] = ilcs_code_match.group(1) if ilcs_code_match else None

        # Extract ILCS act title 
        act_title_match = re.search(r'\((.*?)\)\s*\((.*?)\)', full_text)
        data['ilcs_act_title'] = act_title_match.group(2) if act_title_match else None

        # Extract other fields
        title_desc_match = re.search(r'Title:\s*(.*)', full_text, re.DOTALL)
        if title_desc_match:
            title_text = title_desc_match.group(1).split('Cite:')[0].strip()
            data['title_description'] = ' '.join(title_text.split())
        else:
            data['title_description'] = None

        cite_match = re.search(r'Cite:\s*(.*)', full_text)
        data['cite'] = cite_match.group(1).strip() if cite_match else None

        source_match = re.search(r'Source:\s*(.*)', full_text)
        data['source'] = source_match.group(1).strip() if source_match else None

        short_title_match = re.search(r'Short title:\s*(.*)', full_text)
        data['short_title'] = short_title_match.group(1).strip() if short_title_match else None

    df = pd.DataFrame([data])
    return df

def build_acts_text_table(df_urls:pd.DataFrame, parse_fn: Callable[[str], pd.DataFrame], act_label:str="Act (F)") -> pd.DataFrame:
    """
    Build a table of Act text by parsing all Act (F) URLs.

    Parameters:
        df_urls (pd.DataFrame): Input DataFrame containing ILGA URLs.
        parse_fn (callable): Function to parse each Act page.
        act_label (str): Label to filter to Acts. Defaults to "Act (F)".

    Returns:
        pd.DataFrame: Concatenated DataFrame with Act text data.
    """
    # Filter for acts
    df_acts = df_urls[df_urls['ilcs_index_type_label'] == act_label]

    df_acts_text = pd.DataFrame(columns=[
        'ilcs_code',
        'ilcs_act_title',
        'title_description',
        'cite',
        'source',
        'short_title',
        'section_url'
    ])

    # Loop through act URLs
    for url in tqdm(df_acts['section_url'], desc="Parsing Acts"):
        try:
            df_parsed = parse_fn(url_path=url)
            df_parsed['section_url'] = url
            df_acts_text = pd.concat([df_acts_text, df_parsed], ignore_index=True)
        except Exception as e:
            print(f"[ERROR] Failed to parse {url}: {e}")
    
    df_acts_text = pd.merge(left = df_acts_text, right = df_urls[['section_url','section_file']], how='left', on='section_url')

    return df_acts_text

def parse_statute_page(url_path:str, base_url:str="https://ilga.gov") -> pd.DataFrame:
    """
    Extracts Statute text elements from ILGA HTML page using BeautifulSoup
    and organizes them into a pandas DataFrame.

    Args:
        url_path (str): the URL path that follows the base_url.
        base_url (str): top-level domain URL.

    Returns:
        pandas.DataFrame: A DataFrame containing the extracted information
                        for the statute, with columns for ILCS Code,
                        Section Number, Statute Text,
                        Source, and an Amended Statute Flag.
    """

    response = _request_util(url=f'{base_url}{url_path}')
    soup = BeautifulSoup(response.text, 'html.parser')

    data = {
        'ilcs_code': None,
        'section_number': None,
        'statute_text': '',
        'source': None,
        'amended_statute': False
    }

    # Extract ILCS code 
    ilcs_code_match = soup.find(string=re.compile(r'\(?\d+\s+ILCS\s+\d+/\d+[\w\-.]*\)?'))
    if ilcs_code_match:
        ilcs_code_clean = re.search(r'\d+\s+ILCS\s+\d+/\d+[\w\-.]*', ilcs_code_match)
        if ilcs_code_clean:
            data['ilcs_code'] = ilcs_code_clean.group(0)

    # Get all text
    all_text = soup.body.get_text(separator='\n', strip=True)

    # Indicate amended statute
    if "Text of Section after amendment" in all_text:
        data['amended_statute'] = True
        all_text = all_text.split("Text of Section after amendment", 1)[1]

    lines = all_text.split('\n')

    found_section = False
    found_statute_text = False
    statute_text_lines = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Extract section number
        if line.startswith('Sec.') and not found_section:
            parts = line.split('.', 1)
            if len(parts) > 1:
                data['section_number'] = parts[1].strip()
            else:
                data['section_number'] = line
            found_section = True
            found_statute_text = True
            continue

        # Extract source
        if line.startswith('(Source:'):
            data['source'] = line
            found_statute_text = False
            continue

        # Collect statute text
        if found_statute_text:
            original_element = soup.find(string=re.compile(re.escape(line)))
            if original_element:
                leading_whitespace = ''
                for sibling in original_element.parent.previous_siblings:
                    if isinstance(sibling, str):
                        leading_whitespace = sibling + leading_whitespace
                    else:
                        break
                indent_spaces = leading_whitespace.replace('\xa0', ' ').count(' ')
                statute_text_lines.append(' ' * indent_spaces + line)
            else:
                statute_text_lines.append(line)

    data['statute_text'] = '\n'.join(statute_text_lines).strip()

    return pd.DataFrame([data])

def build_statutes_text_table(df_urls:str, parse_fn: Callable[[str], pd.DataFrame], section_label:str="Section (K)") -> pd.DataFrame:
    """
    Build a table of Statute text by parsing all Section (K) URLs.

    Parameters:
        df_urls (pd.DataFrame): Input DataFrame containing ILGA URLs.
        parse_fn (callable): Function to parse each Statute page.
        section_label (str): Label to filter sections. Defaults to "Section (K)".

    Returns:
        pd.DataFrame: Concatenated DataFrame with Statute text data.
    """
    # Filter for sections
    df_statutes = df_urls[df_urls['ilcs_index_type_label'] == section_label]

    df_statutes_text = pd.DataFrame(columns=[
        'ilcs_code',
        'section_number',
        'statute_text',
        'source',
        'amended_statute',
        'section_url'
    ])

    # Loop through section URLs 
    for url in tqdm(df_statutes['section_url'], desc="Parsing Statutes"):
        try:
            df_parsed = parse_fn(url_path=url)
            df_parsed['section_url'] = url
            df_statutes_text = pd.concat([df_statutes_text, df_parsed], ignore_index=True)
        except Exception as e:
            print(f"[ERROR] Failed to parse {url}: {e}")

    df_statutes_text = pd.merge(left = df_statutes_text, right = df_urls[['section_url','section_file']], how='left', on='section_url')

    return df_statutes_text

# %% 
# Build ILCS URL index
df_ilga_urls = build_ilcs_index()
df_ilga_urls.to_parquet('/mnt/c/Users/nicholasmarchio/OneDrive - Cook County Government/Desktop/projects/statute-xwalk/ilcs-links.parquet')
df_ilga_urls.to_csv('/mnt/c/Users/nicholasmarchio/OneDrive - Cook County Government/Desktop/projects/statute-xwalk/ilcs-links.csv')

# %%
df_ilga_urls = pd.read_parquet('/mnt/c/Users/nicholasmarchio/OneDrive - Cook County Government/Desktop/projects/statute-xwalk/ilcs-links.parquet')

# %%
# Build table of ILCS Act text
df_acts_text = build_acts_text_table(
    df_urls=df_ilga_urls,
    parse_fn=parse_act_page)

# %%
# Write the Act text data
df_acts_text.to_parquet('/mnt/c/Users/nicholasmarchio/OneDrive - Cook County Government/Desktop/projects/statute-xwalk/ilcs-act-text.parquet')
df_acts_text.to_csv('/mnt/c/Users/nicholasmarchio/OneDrive - Cook County Government/Desktop/projects/statute-xwalk/ilcs-act-text.csv')

# %%
# Subset of most relevant chapters and acts
chapter_act_list = pd.DataFrame({
    'chapter_num': ['0010', '0015', '0020', '0020', '0020', '0020', '0035', '0035', '0035', '0040', '0055', '0065', '0070', '0105', '0210', '0225', '0225', '0225', '0225', '0225', '0225', '0225', '0225', '0225', '0225', '0225', '0225', '0230', '0230', '0235', '0305', '0320', '0325', '0410', '0415', '0415', '0425', '0430', '0430', '0430', '0510', '0510', '0510', '0515', '0520', '0605', '0605', '0610', '0610', '0620', '0625', '0625', '0625', '0625', '0705', '0720', '0720', '0720', '0720', '0720', '0720', '0720', '0720', '0720', '0720', '0720', '0720', '0720', '0720', '0720', '0720', '0720', '0725', '0725', '0725', '0725', '0730', '0730', '0730', '0730', '0740', '0740', '0740', '0750', '0750', '0750', '0760', '0765', '0805', '0815', '0815', '0820'],
    'act_num':     ['0005', '0335', '0505', '0835', '2305', '2615', '0120', '0130', '0135', '0005', '0005', '0005', '1505', '0005', '0032', '0041', '0051', '0057', '0065', '0115', '0210', '0447', '0510', '0605', '0650', '0735', '0740', '0005', '0010', '0005', '0005', '0020', '0005', '0620', '0060', '0105', '0035', '0065', '0066', '0085', '0005', '0068', '0070', '0005', '0005', '0005', '0010', '0090', '0095', '0005', '0005', '0025', '0040', '0045', '0405', '0005', '0024', '0125', '0130', '0135', '0150', '0215', '0250', '0360', '0515', '0550', '0570', '0600', '0635', '0646', '0685', '0690', '0005', '0145', '0195', '0225', '0005', '0148', '0150', '0154', '0021', '0022', '0090', '0016', '0045', '0060', '0055', '1040', '0405', '0350', '0515', '0160']
    })
chapter_act_list['ilcs_index_number'] = chapter_act_list['chapter_num'] + chapter_act_list['act_num'] + '0'
df_ilga_urls_subset = pd.merge(left = df_ilga_urls, right = chapter_act_list, how='inner', on='ilcs_index_number')

# %%
# Build table of ILCS Statute text
df_statutes_text = build_statutes_text_table(
    df_urls=df_ilga_urls_subset,
    parse_fn=parse_statute_page)

# %%
# Write the Statute text data
df_statutes_text.to_parquet('/mnt/c/Users/nicholasmarchio/OneDrive - Cook County Government/Desktop/projects/statute-xwalk/ilcs-statutes-text.parquet')
df_statutes_text.to_csv('/mnt/c/Users/nicholasmarchio/OneDrive - Cook County Government/Desktop/projects/statute-xwalk/ilcs-statutes-text.csv')

# %%
# Read all files
df_ilga_urls = pd.read_parquet('/mnt/c/Users/nicholasmarchio/OneDrive - Cook County Government/Desktop/projects/statute-xwalk/ilcs-links.parquet')
df_acts_text = pd.read_parquet('/mnt/c/Users/nicholasmarchio/OneDrive - Cook County Government/Desktop/projects/statute-xwalk/ilcs-act-text.parquet')
df_statutes_text = pd.read_parquet('/mnt/c/Users/nicholasmarchio/OneDrive - Cook County Government/Desktop/projects/statute-xwalk/ilcs-statutes-text.parquet')

