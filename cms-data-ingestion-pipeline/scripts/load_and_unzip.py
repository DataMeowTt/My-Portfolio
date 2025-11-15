import os
import requests
import zipfile

URL = "https://download.open.fda.gov/drug/ndc/drug-ndc-0001-of-0001.json.zip"
output_dir = "/opt/airflow/data/NDC_DATA"
zip_file = f'{output_dir}/drug-ndc-0001-of-0001.json.zip'

def download_and_unzip():
    os.makedirs(output_dir, exist_ok=True)
    response = requests.get(URL, stream=True) # download header to avoid loading entire file into memory
    response.raise_for_status()  # ensure we notice bad responses

    try:
        with open(zip_file, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192): # write in 8KB chunks to save memory
                file.write(chunk)
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(output_dir)