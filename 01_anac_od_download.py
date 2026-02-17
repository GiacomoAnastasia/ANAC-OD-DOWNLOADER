"""
01_anac_od_download.py
Author: R. Nai
Creation date: 10/01/2024
Last modified: 01/03/2024 (added class SSLAdapter)
Description: application to download public notices from the ANAC website
https://dati.anticorruzione.it/opendata/download/dataset/cig-AAAA/filesystem/cig_csv_YYYY_MM.zip
(ex. https://dati.anticorruzione.it/opendata/download/dataset/cig-2021/filesystem/cig_csv_2021_01.zip)

[2025-06-12]: updated with logging functionalities.
[2025-02-17]: updated merge_csv_files function to include a summary of the merge results and added logging.
"""

### IMPORT ###
import logging
import re
from datetime import datetime
from pathlib import Path

### LOCAL IMPORT ###
from config import config_reader
from utility_manager.utilities import check_and_create_directory, url_download, url_unzip, read_urls_from_json

### GLOBALS ###
yaml_config = config_reader.config_read_yaml("config.yml", "config")
list_months = [f"{i:02}" for i in range(1, 13)]
# url_base = str(yaml_config["DOWNLOAD_URL"])
year_start = int(yaml_config["YEAR_START_DOWNLOAD"])
year_end = int(yaml_config["YEAR_END_DOWNLOAD"]) 
url_statics_file = str(yaml_config["ANAC_STATIC_URLS_JSON"])
url_dynamic_file = str(yaml_config["ANAC_DYNAMIC_URLS_JSON"])
prefixes_json_file = str(yaml_config["ANAC_PREFIXES_JSON"])
cig_prefix = str(yaml_config["CIG_PREFIX"])
anac_other_dataset_names = yaml_config.get("ANAC_OTHER_DATASET_NAMES", [])

merge_do = bool(yaml_config.get("MERGE_DO", False))  # whether to merge the CSV files after download and unzip or not
unzip_do = bool(yaml_config.get("UNZIP_DO", False))  # whether to unzip the downloaded files or not

# OUTPUT
merge_file = f"bando_cig_{year_start}-{year_end}.csv" # final file with all the tenders following years
anac_download_dir = str(yaml_config["ANAC_DOWNLOAD_DIR"]) 
data_dir = str(yaml_config["OD_ANAC_DIR"])

### FUNCTIONS ###

def url_generate(year_start: int, year_end: int, list_months: list, url_base: list, key: str, day: str = "01") -> list:
    """
    Generates a list of URLs based on a range of years, a list of months, and a base URL.

    Parameters:
        year_start (int): the starting year of the range (inclusive).
        year_end (int): the ending year of the range (inclusive). It assumes `year_end` >= `year_start`.
        list_months (list): a list of strings representing months, where each month is expected to be in a format that matches the expected URL pattern (e.g., '01' for January).
        url_base (str): The base URL to which the year and month will be appended. The base URL should not end with a slash.
        key (str): the dataset name key to be used in URL construction (e.g., "cig" or other dataset names).
        day (str): the day to be used in URL construction (default: "01").
        
    Returns:
    - list: a list of strings, where each string is a fully constructed URL according to the described pattern.
    """

    list_url = []
    for year in range(year_start, year_end + 1):  # year_end+1 to keep year_end inclusive
        year_str = f"{year:04}"
        for month in list_months:
            month_str = f"{int(month):02}"
            day_str = f"{int(day):02}"
            for pattern in url_base:
                url = (
                    pattern
                    .replace("{YYYY}", year_str)
                    .replace("{MM}", month_str)
                    .replace("{DD}", day_str)
                )
                if "{dataset-name}" in url:
                    url = url.replace("{dataset-name}", key)
                list_url.append(url)
    return list_url

def merge_csv_files(source_dir: str, output_dir:str, prefixes: list) -> dict:
    """
    Merges CSV files in the source directory by dataset prefix into one merged file per prefix.
    
    Parameters:
        source_dir (str): the path to the directory containing the CSV files to be merged.
        output_dir (str): the path to the output CSV directory where the merged content will be stored.
        prefixes (list): list of dataset prefixes to use for grouping and merging.

    Returns:
        dict: merge summary by prefix.
    """

    source_path = Path(source_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    def normalize_prefix(file_stem: str) -> str:
        normalized = re.sub(r'^\d{8}-', '', file_stem)
        normalized = re.sub(r'_\d{4}_\d{2}$', '', normalized)
        return normalized

    # Ensure the source directory exists
    if not source_path.is_dir():
        print(f"WARNING! Source directory {source_dir} does not exist.")
        return {}

    if not prefixes:
        print("WARNING! No prefixes provided for merging.")
        return {}

    all_csv_files = sorted(source_path.glob('*.csv'))
    merge_result = {}

    for prefix_name in prefixes:
        matched_files = [
            csv_file for csv_file in all_csv_files
            if normalize_prefix(csv_file.stem) == prefix_name
        ]

        if not matched_files:
            continue

        output_file = f"{prefix_name}.csv"
        current_output_path = output_path / output_file

        with current_output_path.open(mode='w') as outfile:
            for csv_file in matched_files:
                with csv_file.open(mode='r') as infile:
                    outfile.write(infile.read())
                print(f"Merged [{prefix_name}]: {csv_file.name}")

        with current_output_path.open(mode='r') as file:
            lines = sum(1 for _ in file)

        merge_result[prefix_name] = {
            "output_file": str(current_output_path),
            "files_merged": len(matched_files),
            "lines": lines
        }

        print(
            f"Prefix '{prefix_name}' merged into '{output_file}' "
            f"with {len(matched_files)} files and {lines} lines."
        )

    return merge_result

def print_list_urls(list_urls: list) -> None:
    """
    Prints each URL in the provided list of URLs.

    Parameters:
        list_urls (list): A list of URLs to be printed.
    Returns:
        None
    """
    print("List of URLs:")
    for i, url in enumerate(list_urls, start=1):
        print(f"{i}) {url}")

### MAIN ###

def main() -> None:
    """
    Main script function.
    Parameters: None
    Returns: None
    """

    # Logging setup
    log_file = f"{Path(__file__).stem}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    
    print()
    print("*** PROGRAM START ***")
    print()
    
    logger.info("PROGRAM START")

    start_time = datetime.now().replace(microsecond=0)
    print("Start process: " + str(start_time))
    logger.info(f"Start process: {start_time}")
    print()

    print(">> Generating output directories")
    check_and_create_directory(anac_download_dir)
    check_and_create_directory(data_dir)
    print()
    
    print(">> Generating dynamic URLs")
    url_base = read_urls_from_json(url_dynamic_file, "cig")
    list_urls_din = url_generate(year_start, year_end, list_months, url_base, "cig")
    list_urls_din_len = len(list_urls_din)
    print("URLs generated (num):", list_urls_din_len)
    print_list_urls(list_urls_din) # debug
    print()

    print(">> Generating dynamic URLs (others)")
    url_base_others = read_urls_from_json(url_dynamic_file, "others")
    list_urls_others_din = []
    for dataset_name in anac_other_dataset_names:
        list_urls_others_din.extend(
            url_generate(year_start, year_end, list_months, url_base_others, dataset_name)
        )
    list_urls_others_din_len = len(list_urls_others_din)
    print("URLs generated (num):", list_urls_others_din_len)
    print_list_urls(list_urls_others_din) # debug
    print()

    print(">> Generating static URLs")
    list_urls_sta = read_urls_from_json(url_statics_file, "others")
    list_urls_sta_len = len(list_urls_sta)
    print("URLs generated (num):", list_urls_sta_len)
    print_list_urls(list_urls_sta) # debug
    print()

    print(">> Merging dynamic and static URLs lists")
    list_urls_all = list_urls_din + list_urls_others_din + list_urls_sta
    list_urls_all_len = len(list_urls_all)
    print("URLs generated (all):", list_urls_all_len)
    # print(list_urls_all) # debug
    print()

    print(">> Downloading from URLs")
    print("Download directory:", anac_download_dir)
    logger.info(f"Starting download from {list_urls_all_len} URLs")
    dic_result = url_download(list_urls_all, anac_download_dir)
    print("Download results")
    print(dic_result)
    logger.info(f"Download completed - Results: {dic_result}")
    print()

    if unzip_do == False:
        print(">> Unzipping skipped as per configuration (UNZIP_DO = False).")
    else:
        print(">> Unzipping files")
        unzipped_files, unzipped_files_error = url_unzip(anac_download_dir)
        print("Unzipped files:", len(unzipped_files))
        print("Files with errors during unzipping:", len(unzipped_files_error))
    print()

    if merge_do == False:
        print(">> Merging skipped as per configuration (MERGE_DO = False).")
    else:
        print(">> Merging files")
        print(">> Reading merge prefixes from JSON")
        prefixes = read_urls_from_json(prefixes_json_file, "prefixes")

        if not prefixes:
            print(f"WARNING! No prefixes found in '{prefixes_json_file}'.")
            logger.warning(f"No prefixes found in '{prefixes_json_file}'.")
        else:
            prefixes_len = len(prefixes)
            print(f"Prefixes found (num): {prefixes_len}")
            logger.info(f"Prefixes found for merging: {prefixes_len}")
            merge_result = merge_csv_files(anac_download_dir, data_dir, prefixes)
            if not merge_result:
                print("WARNING! No CSV files matched the provided prefixes.")
                logger.warning("No CSV files matched the provided prefixes.")
            else:
                print("Merge summary by prefix:")
                for prefix, result in merge_result.items():
                    print(
                        f"- {prefix}: files merged={result['files_merged']}, "
                        f"lines={result['lines']}, output={result['output_file']}"
                    )
                    logger.info(
                        f"- {prefix}: files merged={result['files_merged']}, "
                        f"lines={result['lines']}, output={result['output_file']}"
                    )
                logger.info(f"Merge completed for {len(merge_result)} prefixes.")
        print()

    # end
    end_time = datetime.now().replace(microsecond=0)
    delta_time = end_time - start_time

    print()
    print("End process:", end_time)
    print("Time to finish:", delta_time)
    logger.info(f"End process: {end_time}")
    logger.info(f"Time to finish: {delta_time}")
    print()

    print()
    print("*** PROGRAM END ***")
    logger.info("PROGRAM END")
    print()

if __name__ == "__main__":
    main()