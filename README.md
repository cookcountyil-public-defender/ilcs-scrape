# Illinois General Assembly Statute Scraper
This Python script is designed to crawl the Illinois General Assembly (ILGA) website, specifically the FTP directory for Illinois Compiled Statutes (ILCS), and extract structured information about chapters, acts, and individual statutes. It parses HTML content to gather details such as ILCS codes, section numbers, statute text, and act titles, saving the extracted data into Parquet and CSV files for further analysis.

## Key Functions
* `_request_util:` Handles HTTP GET requests with retry logic and error handling.
* `_get_pages:` Fetches and parses links from a given ILGA FTP page.
* `_parse_filestring:` Parses a statute filename into its components (ILCS code, statute outline level, section number).
* `build_ilcs_index:` The main function to crawl the ILGA site and build a comprehensive index of all ILCS URLs.
* `parse_act_page:` Parses a single Act page to extract its title, description, cite, source, and short title.
* `build_acts_text_table:` Builds a DataFrame of act texts by applying parse_act_page to all identified act URLs.
* `parse_statute_page:` Parses a single Statute page to extract its ILCS code, section number, full text, source, and an amended statute flag.
* `build_statutes_text_table:` Builds a DataFrame of statute texts by applying parse_statute_page to all identified statute URLs.

## Output Files
The script generates the following files in the specified directory (/mnt/c/Users/nicholasmarchio/OneDrive - Cook County Government/Desktop/projects/statute-xwalk/ in the example):
* ilcs-links.parquet and ilcs-links.csv: Contains the comprehensive index of all discovered ILCS chapter, act, and section URLs.
* ilcs-act-text.parquet and ilcs-act-text.csv: Contains extracted text and metadata for each ILCS Act.
* ilcs-statutes-text.parquet and ilcs-statutes-text.csv: Contains extracted text and metadata for each ILCS Statute (from the specified subset).
* Link to [results](https://docs.google.com/spreadsheets/d/1CMfkzViiVkZ3Zvy14T_m3MxwgHwEOV3BbDiSEVxw4kI/edit?usp=sharing).
