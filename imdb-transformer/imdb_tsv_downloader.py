import os
import requests
import gzip

# URLs der Gzip-Dateien
url_list = [
    "https://datasets.imdbws.com/name.basics.tsv.gz",
]

# Speicherpfad der Gzip-Dateien
file_path_list = [
    "./tsv_dump/name.basics.tsv.gz",
]

# Überprüfe, ob die Gzip-Dateien vorhanden sind und lade sie herunter, falls nicht
for url, file_path in zip(url_list, file_path_list):
    if not os.path.isfile(file_path):
        print(f"{file_path} not found, downloading...")
        response = requests.get(url)
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"{file_path} downloaded.")
    else:
        print(f"{file_path} already exists.")

# Entpacke die Gzip-Dateien
for file_path in file_path_list:
    with gzip.open(file_path, "rb") as f_in:
        with open(os.path.splitext(file_path)[0], "wb") as f_out:
            f_out.write(f_in.read())
    print(f"{file_path} unpacked.")
