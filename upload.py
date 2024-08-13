import requests
import os
from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper
import time

import argparse

# Constants
ZENODO_API_URL = "https://zenodo.org/api/deposit/depositions"
ACCESS_TOKEN = "Your Token"
HEADERS = {"Content-Type": "application/json"}


def get_deposition(deposition_id):
    url = f"{ZENODO_API_URL}/{deposition_id}"
    response = requests.get(url, params={"access_token": ACCESS_TOKEN})
    response.raise_for_status()
    return response.json()


def create_deposition():
    url = f"{ZENODO_API_URL}"
    print("Creating new deposition...")
    title = input("Enter the title of the dataset: ")
    json = {
        "metadata": {
            "title": title,
            "upload_type": "dataset",
        }
    }
    response = requests.post(url, params={"access_token": ACCESS_TOKEN}, json=json, headers=HEADERS)
    response.raise_for_status()
    return response.json()


def upload_file(bucket_url, file_path):
    print(f"Uploading {file_path}...")
    filename = os.path.basename(file_path)
    file_size = os.stat(file_path).st_size
    url = f"{bucket_url}/{filename}"
    while True:
        try:
            with open(file_path, "rb") as f:
                with tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1024) as t:
                    file = CallbackIOWrapper(t.update, f, "read")
                    response = requests.put(url, data=file, params={"access_token": ACCESS_TOKEN})
        except KeyboardInterrupt:
            print("Manual interrupt. Exiting...")
            exit()
        except:
            # wait for 5 seconds before retrying
            time.sleep(5)
            continue
        break

    response.raise_for_status()
    return response.json()


def main(deposition_id, directory, overwrite=False):
    if deposition_id:
        deposition = get_deposition(deposition_id)
        print(f"Found deposition with ID: {deposition_id}: {deposition['title']}")
        bucket_url = deposition["links"]["bucket"]
    else:
        deposition = create_deposition()
        deposition_id = deposition["id"]
        print(f"Created new deposition with ID: {deposition_id}.")
        bucket_url = deposition["links"]["bucket"]

    files = sorted([str(f) for f in Path(directory).rglob('*') if f.is_file()])

    if not overwrite:
        existing_files = [f["filename"] for f in deposition["files"]]
        files = [f for f in files if os.path.basename(f) not in existing_files]

    print(f"Uploading {len(files)} files to deposition ID: {deposition_id}...")
    print(f"Files: {files}")

    # Upload each file part
    for file_path in files:
        upload_file(bucket_url, file_path)

    print(f"All files uploaded successfully to deposition ID: {deposition_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload files to Zenodo")
    parser.add_argument(
        "--id",
        type=str,
        help="Zenodo deposition ID (ignore to create a new deposition)",
        dest="deposition_id",
    )
    parser.add_argument(
        "--dir", type=str, required=True, help="Directory containing the files", dest="directory"
    )
    parser.add_argument(
        "--overwrite", type=bool, default=False, help="T/F overwrite your file in current deposition with same name"
    )
    args = parser.parse_args()

    main(args.deposition_id, args.directory, args.overwrite)
