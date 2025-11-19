"""
Coletor autônomo de fontes técnicas para o projeto Xeque-Mate.
Agora convertido em função reutilizável:

    run_autonomous_collector(base_output_dir)

Ele cria:

    fase_nist/
    status_incidente/
    orientacao_nist/
    acao_recomendada/
    suporte_analista/
    iocs_relacionados/
    gravidade/
    resumo_evento_rag/

e baixa todo o conteúdo técnico automaticamente.
"""

from __future__ import annotations

import re
import time
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup


# =====================================================================
#  UTILITÁRIOS
# =====================================================================

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}


def _get_soup(url: str, params: dict | None = None) -> BeautifulSoup:
    resp = requests.get(url, params=params, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def _download_binary(url: str, dest: Path):
    print(f"[DOWN] {url} -> {dest}")
    with requests.get(url, headers=HEADERS, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    print(f"[OK] {dest}")


def _download_text(url: str, dest: Path, max_retries: int = 2):
    """Baixa HTML com fallback (403/404 gera arquivo de erro)."""
    print(f"[DOWN-HTML] {url} -> {dest}")

    for attempt in range(max_retries):
        try:
            if attempt > 0:
                time.sleep(2 * attempt)

            resp = requests.get(url, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            dest.write_text(resp.text, encoding="utf-8")
            print(f"[OK] {dest}")
            return

        except requests.exceptions.HTTPError as e:
            code = e.response.status_code

            if code in (403, 404):
                print(f"[ERRO] HTTP {code} em {url}")
                dest.write_text(
                    f"# ERRO HTTP {code}\n# URL: {url}\n# {e}\n",
                    encoding="utf-8"
                )
                return

        except requests.exceptions.RequestException as e:
            print(f"[WARN] Erro de conexão: {e}")

    print(f"[ERRO] Falha permanente -> {dest}")
    dest.write_text(f"# ERRO PERMANENTE: {url}\n", encoding="utf-8")


# =====================================================================
#  NIST
# =====================================================================

NIST_BASE = "https://csrc.nist.gov"


def nist_search_detail_url_sp(sp_number: str) -> str | None:
    """Acha URL do SP 800-XX mais recente."""
    search_url = f"{NIST_BASE}/publications/search"
    soup = _get_soup(search_url, params={"keywords-lg": f"800-{sp_number}"})

    hrefs = [
        a["href"]
        for a in soup.find_all("a", href=True)
        if f"/pubs/sp/800/{sp_number}/" in a["href"] and a["href"].endswith("/final")
    ]

    if not hrefs:
        return None

    best = max(
        hrefs,
        key=lambda h: int(re.search(r"/r(\d+)/final$", h).group(1)) if re.search(r"/r(\d+)/final$", h) else 0
    )

    return urljoin(NIST_BASE, best)


def nist_download_pdf_from_detail(detail_url: str, dest_dir: Path):
    soup = _get_soup(detail_url)

    pdf_link = None
    for a in soup.find_all("a", href=True):
        if "Download URL" in a.get_text(strip=True):
            pdf_link = a["href"]
            break

    if not pdf_link:
        print(f"[WARN] Sem PDF em {detail_url}")
        return

    pdf_url = pdf_link if pdf_link.startswith("http") else urljoin(detail_url, pdf_link)
    _download_binary(pdf_url, dest_dir / pdf_url.split("/")[-1])


# =====================================================================
#  MITRE
# =====================================================================

MITRE_BASE = "https://attack.mitre.org"


def collect_mitre_tactic(tactic_id: str, dest_dir: Path):
    url = f"{MITRE_BASE}/tactics/{tactic_id}/"
    _download_text(url, dest_dir / f"{tactic_id}.html")


# =====================================================================
#  Fonte genérica (Elastic, Splunk, QRadar)
# =====================================================================

def collect_simple_html(url: str, dest_dir: Path, filename_prefix: str):
    _download_text(url, dest_dir / f"{filename_prefix}.html")


# =====================================================================
#  FUNÇÕES DE COLETA PARA CADA CAMPO
# =====================================================================

def collect_for_fase_nist(dir_path: Path):
    detail = nist_search_detail_url_sp("61")
    if detail:
        nist_download_pdf_from_detail(detail, dir_path)


def collect_for_status_incidente(dir_path: Path):
    collect_simple_html("https://www.elastic.co/guide/index.html", dir_path, "elastic_docs")
    collect_simple_html("https://docs.splunk.com/", dir_path, "splunk_docs")
    collect_simple_html("https://www.ibm.com/docs/en/qsip/7.5", dir_path, "qradar_docs")


def collect_for_orientacao_nist(dir_path: Path):
    _download_binary(
        "https://csrc.nist.gov/extensions/nudp/services/json/csf/download?olirids=all",
        dir_path / "csf_core_v2.0.json"
    )
    collect_simple_html("https://www.nist.gov/cyberframework", dir_path, "cyberframework")


def collect_for_acao_recomendada(dir_path: Path):
    detail = nist_search_detail_url_sp("61")
    if detail:
        nist_download_pdf_from_detail(detail, dir_path)


def collect_for_suporte_analista(dir_path: Path):
    collect_mitre_tactic("TA0043", dir_path)


def collect_for_iocs_relacionados(dir_path: Path):
    dir_path.joinpath("LEIA-ME.txt").write_text(
        "Ferramentas IOC:\n- MISP\n- OTX\n- VirusTotal\n",
        encoding="utf-8"
    )


def collect_for_gravidade(dir_path: Path):
    detail = nist_search_detail_url_sp("30")
    if detail:
        nist_download_pdf_from_detail(detail, dir_path)


def collect_for_resumo_evento_rag(dir_path: Path):
    detail = nist_search_detail_url_sp("61")
    if detail:
        nist_download_pdf_from_detail(detail, dir_path)


# =====================================================================
#  FUNÇÃO PRINCIPAL (MODULAR)
# =====================================================================

def run_autonomous_collector(base_output_dir: str | Path):
    """
    Executa TODO o coletor autônomo de fontes técnicas do Xeque-Mate.

    Parâmetro:
        base_output_dir: diretório de saída onde todas as pastas serão criadas.
    """
    base_output_dir = Path(base_output_dir)
    base_output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[INFO] Coletor autônomo – saída: {base_output_dir}\n")

    # Campos definidos no modelo
    fields = {
        "fase_nist": collect_for_fase_nist,
        "status_incidente": collect_for_status_incidente,
        "orientacao_nist": collect_for_orientacao_nist,
        "acao_recomendada": collect_for_acao_recomendada,
        "suporte_analista": collect_for_suporte_analista,
        "iocs_relacionados": collect_for_iocs_relacionados,
        "gravidade": collect_for_gravidade,
        "resumo_evento_rag": collect_for_resumo_evento_rag,
    }

    # Executa cada coletor
    for field_name, func in fields.items():
        print(f"\n=== {field_name} ===")
        field_dir = base_output_dir / field_name
        field_dir.mkdir(parents=True, exist_ok=True)
        func(field_dir)

    print("\n[OK] Coleta completa!\n")


# =====================================================================
# Modo CLI opcional
# =====================================================================

if __name__ == "__main__":
    run_autonomous_collector("data/external")
