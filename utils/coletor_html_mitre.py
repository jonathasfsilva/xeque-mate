import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path

BASE_URL = "https://attack.mitre.org/"


# ------------------------------
#  UTILIDADES
# ------------------------------

def safe_filename(name: str) -> str:
    """Transforma IDs em nomes válidos de arquivo."""
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


# ------------------------------
#  EXTRATORES
# ------------------------------

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
    """Coleta técnicas associadas a uma tática."""
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
    """Coleta sub-técnicas da técnica."""
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

def run_mitre_collector(output_dir: str | Path, limit: int | None = None):
    """
    Executa o coletor MITRE ATT&CK completo.
    Pode ser chamado de qualquer lugar do sistema.

    Parâmetros:
        output_dir (str | Path): diretório onde os HTMLs serão salvos.
        limit (int | None): número máximo de HTMLs a coletar. 
                            Se None, coleta tudo.
    """
    output_dir = Path(output_dir)

    print("\n=== COLETOR HTML MITRE ATT&CK ===\n")
    print(f"Saída configurada para: {output_dir}")
    print(f"Limite de coleta: {limit if limit else 'SEM LIMITE'}\n")

    count = 0  # contador global

    # Função auxiliar interna
    def _check_limit():
        nonlocal count
        if limit is not None and count >= limit:
            print(f"\n[!] Limite de {limit} arquivos atingido. Coleta encerrada.")
            return True
        return False

    # 1. Coletar táticas
    tactics = extract_tactics()

    for tactic in tactics:
        if _check_limit():
            return

        html = fetch_html(tactic["url"])
        save_html(html, output_dir / "tactics" / safe_filename(tactic["id"]))
        count += 1

        # 2. Coletar técnicas
        techniques = extract_techniques(tactic["url"])

        for tech in techniques:
            if _check_limit():
                return

            tech_html = fetch_html(tech["url"])
            save_html(tech_html, output_dir / "techniques" / safe_filename(tech["id"]))
            count += 1

            # 3. Coletar sub-técnicas
            subtechs = extract_subtechniques(tech["url"])

            for sub in subtechs:
                if _check_limit():
                    return

                sub_html = fetch_html(sub["url"])
                save_html(sub_html, output_dir / "techniques" / safe_filename(sub["id"]))
                count += 1

    print("\n=== FINALIZADO COM SUCESSO ===")
    print(f"Total coletado: {count}")
    print(f"HTMLs salvos em: {output_dir}/")

