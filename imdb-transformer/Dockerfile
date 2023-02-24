# Basis-Image
FROM python:3.9

# Arbeitsverzeichnis
WORKDIR /app

# Kopiere die Anforderungen ins Image
COPY requirements.txt .

# Installiere die erforderlichen Python-Pakete
RUN pip install -r requirements.txt

# Kopiere den Rest des Codes ins Image
# COPY . .

# Führe das Python-Skript aus
CMD ["python", "create_database.py"]
