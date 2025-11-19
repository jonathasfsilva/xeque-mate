import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path

BASE_URL = "https://attack.mitre.org/"

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "coletor_data" / "data" / "mitre_html"

def safe_filename(name: str) -> str:
    """Converte IDs em nomes válidos de arquivo."""
    return name.replace("/", "_").replace("\\", "_") + ".html"

def save_html(content: str, path: str):
    """Salva o HTML bruto no arquivo informado."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"[✔] Salvo: {path}")

def fetch_html(url: str) -> str:
    """Baixa o HTML de uma página MITRE."""
    print(f"[*] Baixando: {url}")
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.text

def extract_tactics():
    """Coleta todas as táticas do MITRE."""
    page = fetch_html(urljoin(BASE_URL, "tactics/enterprise/"))
    soup = BeautifulSoup(page, "html.parser")

    table = soup.find("table")
    rows = table.find_all("tr")[1:]

    tactics = []

    for row in rows:
        cols = row.find_all("td")
        link = cols[0].find("a")
        tactic_id = link.text.strip()
        tactic_url = urljoin(BASE_URL, link["href"])

        tactics.append({"id": tactic_id, "url": tactic_url})

    return tactics

def extract_techniques(tactic_url):
    """Coleta todas as técnicas associadas a uma tática."""
    page = fetch_html(tactic_url)
    soup = BeautifulSoup(page, "html.parser")

    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")[1:]
    techniques = []

    for row in rows:
        cols = row.find_all("td")
        link = cols[0].find("a")
        if not link:
            continue

        tech_id = link.text.strip()
        tech_url = urljoin(BASE_URL, link["href"])
        techniques.append({"id": tech_id, "url": tech_url})

    return techniques

def extract_subtechniques(tech_url):
    """Coleta sub-técnicas, se existirem."""
    page = fetch_html(tech_url)
    soup = BeautifulSoup(page, "html.parser")

    table = soup.find("table", {"class": "table"})
    if not table:
        return []

    rows = table.find_all("tr")[1:]
    subtechs = []

    for row in rows:
        cols = row.find_all("td")
        link = cols[0].find("a")
        if not link:
            continue

        sub_id = link.text.strip()
        sub_url = urljoin(BASE_URL, link["href"])
        subtechs.append({"id": sub_id, "url": sub_url})

    return subtechs

def run():
    print("\n=== COLETOR HTML MITRE ATT&CK ===\n")

    # 1. Coletar táticas
    tactics = extract_tactics()

    for tactic in tactics:
        # Baixar HTML da tática
        html = fetch_html(tactic["url"])
        save_html(html, os.path.join(OUTPUT_DIR, "tactics", safe_filename(tactic["id"])))

        # 2. Coletar técnicas da tática
        techniques = extract_techniques(tactic["url"])

        for tech in techniques:
            # Baixar HTML da técnica
            tech_html = fetch_html(tech["url"])
            save_html(tech_html, os.path.join(OUTPUT_DIR, "techniques", safe_filename(tech["id"])))

            # 3. Coletar sub-técnicas da técnica
            subtechs = extract_subtechniques(tech["url"])

            for sub in subtechs:
                sub_html = fetch_html(sub["url"])
                save_html(sub_html, os.path.join(OUTPUT_DIR, "techniques", safe_filename(sub["id"])))

    print("\n=== FINALIZADO COM SUCESSO ===")
    print(f"Todos os HTMLs salvos em: {OUTPUT_DIR}/")

if __name__ == "__main__":
    run()
