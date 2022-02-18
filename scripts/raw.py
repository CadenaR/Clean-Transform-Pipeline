import csv
from zipfile import ZipFile


def save_raw_data(inbound_path, raw_path):
    #  Unzipping file
    with ZipFile(
        inbound_path,
        "r",
    ) as zipfile:
        names_list = zipfile.namelist()
        csv_file_path = zipfile.extract(names_list[0], path=raw_path)

        # Open the CSV file in read mode
        with open(csv_file_path, mode="r", encoding="windows-1252") as csv_file:
            reader = csv.DictReader(csv_file)

            row = next(reader)  # Get first row from reader
            print("[Raw] First row example:", row)


if __name__ == "__main__":
    print("[Raw] Start")
    print("[Raw] Unzipping file")
    inbound_path = '../data/inbound/all_data.zip'
    raw_path = '../data/raw/'
    save_raw_data(inbound_path, raw_path)
    print("[Raw] End")