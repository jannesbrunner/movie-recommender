import os
import requests
import gzip
from tqdm import tqdm


# URLs der Gzip-Dateien
url_list = [
    "https://datasets.imdbws.com/name.basics.tsv.gz",
    "https://datasets.imdbws.com/title.akas.tsv.gz",
    "https://datasets.imdbws.com/title.basics.tsv.gz",
    "https://datasets.imdbws.com/title.crew.tsv.gz",
    "https://datasets.imdbws.com/title.episode.tsv.gz",
    "https://datasets.imdbws.com/title.principals.tsv.gz",
    "https://datasets.imdbws.com/title.ratings.tsv.gz",

]

# Speicherpfad der Gzip-Dateien
file_path_list = [
    "./tsv_dump/name.basics.tsv.gz",
    "./tsv_dump/title.akas.tsv.gz",
    "./tsv_dump/title.basics.tsv.gz",
    "./tsv_dump/title.crew.tsv.gz",
    "./tsv_dump/title.episode.tsv.gz",
    "./tsv_dump/title.principals.tsv.gz",
    "./tsv_dump/title.ratings.tsv.gz",
]

print("IMDb TSV Downloader / Updater v1.0")
print("Downloading and unpacking IMDb TSV files...")

# Überprüfe, ob die Gzip-Dateien vorhanden sind und lade sie herunter, falls nicht
for url, file_path in zip(url_list, file_path_list):
    if not os.path.isfile(file_path):
        print(f"{file_path} not found, downloading...")
        response = requests.get(url, stream=True)
        total_length = response.headers.get('content-length')
        if total_length is None: # no content length header
            with open(file_path, "wb") as f:
                f.write(response.content)
            print(f"{file_path} downloaded.")
        else:
            with open(file_path, "wb") as f:
                dl = 0
                total_length = int(total_length)
                for data in response.iter_content(chunk_size=4096):
                    dl += len(data)
                    f.write(data)
                    done = int(50 * dl / total_length)
                    print(f"\r[{'=' * done}{' ' * (50-done)}] {dl/total_length:.2%}", end="")
            print(f"\r{file_path} downloaded.")
    else:
        print(f"{file_path} already exists.")

# Entpacke die Gzip-Dateien mit progress bar
def ungzip_file(input_path, output_path):
    with gzip.open(input_path, 'rb') as infile:
        with open(output_path, 'wb') as outfile:
            # Bestimme die Größe der gzip-Datei
            file_size = infile.seek(0, 2)
            infile.seek(0)
            # Erstelle den Fortschrittsbalken
            progress_bar = tqdm(total=file_size, unit='B', unit_scale=True)
            # Entpacke die Datei und schreibe sie in die Ausgabedatei
            while True:
                chunk = infile.read(1024)
                if not chunk:
                    break
                outfile.write(chunk)
                # Aktualisiere den Fortschrittsbalken
                progress_bar.update(len(chunk))
            # Schließe den Fortschrittsbalken
            progress_bar.close()
            print(f"{input_path} unpacked.")

for file_path in file_path_list:
    ungzip_file(file_path, file_path[:-3])
    
# löschen der Gzip-Dateien
for file_path in file_path_list:
    os.remove(file_path)
    print(f"{file_path} deleted.")  # Ausgabe: ./tsv_dump/name.basics.tsv.gz deleted.

print("Done.")