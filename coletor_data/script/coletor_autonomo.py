"""
coletor_autonomo.py

Coletor autônomo de fontes técnicas para o projeto Xeque-Mate.

Objetivo:
- Buscar sempre a versão mais recente dos documentos relevantes
  diretamente nos sites oficiais (NIST, MITRE, etc.).
- Salvar os arquivos organizados por CAMPO lógico do modelo:

    fase_nist/
    status_incidente/
    orientacao_nist/
    acao_recomendada/
    suporte_analista/
    iocs_relacionados/
    gravidade/
    resumo_evento_rag/

Uso:
    python -m collectors.coletor_autonomo
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


# =========================================================
# Configuração de paths e campos
# =========================================================

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent  # ajuste se sua estrutura for diferente
BASE_DIR = PROJECT_ROOT / "data" / "external"
print(f"[INFO] Base de dados externa: {BASE_DIR}")

# Campos do seu modelo (tabela do print)
FIELDS = [
    "fase_nist",
    "status_incidente",
    "orientacao_nist",
    "acao_recomendada",
    "suporte_analista",
    "iocs_relacionados",
    "gravidade",
    "resumo_evento_rag",
]

FIELD_DIRS: dict[str, Path] = {}
for field in FIELDS:
    dir_path = BASE_DIR / field
    dir_path.mkdir(parents=True, exist_ok=True)
    FIELD_DIRS[field] = dir_path

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}


# =========================================================
# Utilitários genéricos
# =========================================================

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
    """
    Baixa conteúdo HTML com tratamento de erros.
    Em caso de falha (403, 404, etc), salva um arquivo marcador com o erro.
    """
    print(f"[DOWN-HTML] {url} -> {dest}")
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                time.sleep(2 * attempt)  # backoff progressivo
            
            resp = requests.get(url, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            dest.write_text(resp.text, encoding="utf-8")
            print(f"[OK] {dest}")
            return
            
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            
            if status_code in (403, 404):
                print(f"[ERRO] HTTP {status_code} em {url}")
                print(f"[INFO] Salvando marcador de falha em {dest}")
                
                error_msg = (
                    f"# ERRO AO BAIXAR\n"
                    f"# URL: {url}\n"
                    f"# Código HTTP: {status_code}\n"
                    f"# Erro: {e}\n"
                    f"# Data: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"\n"
                    f"Este arquivo não pôde ser baixado automaticamente.\n"
                    f"Acesse manualmente a URL acima para obter o conteúdo.\n"
                )
                dest.write_text(error_msg, encoding="utf-8")
                return  # Não tenta novamente para 403/404
            else:
                # Outros erros HTTP: tenta novamente
                print(f"[WARN] HTTP {status_code} em {url} (tentativa {attempt + 1}/{max_retries})")
                if attempt == max_retries - 1:
                    raise  # Re-raise no último attempt
                    
        except requests.exceptions.RequestException as e:
            print(f"[WARN] Erro de conexão em {url}: {e} (tentativa {attempt + 1}/{max_retries})")
            if attempt == max_retries - 1:
                print(f"[ERRO] Falha definitiva ao baixar {url}")
                error_msg = f"# ERRO DE CONEXÃO\n# URL: {url}\n# Erro: {e}\n"
                dest.write_text(error_msg, encoding="utf-8")
                return


# =========================================================
# 1) NIST – busca e download de publicações
# =========================================================

NIST_BASE = "https://csrc.nist.gov"


def nist_search_detail_url_sp(sp_number: str) -> str | None:
    """
    Procura a publicação NIST SP 800-XX mais recente (Final) usando a busca do CSRC
    e retorna a URL da página de detalhe.

    Exemplo:
        sp_number="61" -> https://csrc.nist.gov/pubs/sp/800/61/r3/final
    """
    query = f"800-{sp_number}"
    search_url = f"{NIST_BASE}/publications/search"
    print(f"[NIST-SEARCH] SP 800-{sp_number} -> {search_url}")

    soup = _get_soup(search_url, params={"keywords-lg": query})

    hrefs: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if f"/pubs/sp/800/{sp_number}/" in href and href.endswith("/final"):
            hrefs.add(href)

    if not hrefs:
        print(f"[WARN] Nenhuma URL de detalhe encontrada para SP 800-{sp_number}")
        return None

    best_href = None
    best_rev = -1

    for href in hrefs:
        # /pubs/sp/800/61/r3/final -> r3
        m = re.search(r"/r(\d+)/final$", href)
        rev = int(m.group(1)) if m else 0
        if rev > best_rev:
            best_rev = rev
            best_href = href

    if not best_href:
        return None

    detail_url = urljoin(NIST_BASE, best_href)
    print(f"[NIST-DETAIL] SP 800-{sp_number} -> rev {best_rev} -> {detail_url}")
    return detail_url


def nist_search_detail_url_ir(ir_number: str) -> str | None:
    """
    Procura NISTIR (ex.: 8374) mais recente (Final) e retorna a URL de detalhe.
    Exemplo de formatos possíveis:
      - /pubs/ir/8374/final
      - /pubs/ir/8374/r1/final
    """
    search_url = f"{NIST_BASE}/publications/search"
    print(f"[NIST-SEARCH] IR {ir_number} -> {search_url}")

    soup = _get_soup(search_url, params={"keywords-lg": ir_number})

    hrefs: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if f"/pubs/ir/{ir_number}/" in href and href.endswith("/final"):
            hrefs.add(href)
        elif href == f"/pubs/ir/{ir_number}/final":
            hrefs.add(href)

    if not hrefs:
        print(f"[WARN] Nenhuma URL de detalhe encontrada para IR {ir_number}")
        return None

    best_href = None
    best_rev = -1
    for href in hrefs:
        m = re.search(r"/r(\d+)/final$", href)
        rev = int(m.group(1)) if m else 0
        if rev > best_rev:
            best_rev = rev
            best_href = href

    detail_url = urljoin(NIST_BASE, best_href)
    print(f"[NIST-DETAIL] IR {ir_number} -> rev {best_rev if best_rev >= 0 else '0'} -> {detail_url}")
    return detail_url


def nist_download_pdf_from_detail(detail_url: str, dest_dir: Path) -> Path | None:
    """
    Abre a página de detalhe da publicação e baixa o PDF apontado por 'Download URL'.
    """
    print(f"[NIST-DETAIL-PAGE] {detail_url}")
    soup = _get_soup(detail_url)

    pdf_link = None
    for a in soup.find_all("a", href=True):
        if "Download URL" in a.get_text(strip=True):
            pdf_link = a["href"]
            break

    if not pdf_link:
        print(f"[WARN] Não achei link 'Download URL' em {detail_url}")
        return None

    pdf_url = pdf_link if pdf_link.startswith("http") else urljoin(detail_url, pdf_link)
    filename = pdf_url.split("/")[-1]
    dest = dest_dir / filename
    _download_binary(pdf_url, dest)
    return dest


# =========================================================
# 2) MITRE ATT&CK
# =========================================================

MITRE_BASE = "https://attack.mitre.org"


def collect_mitre_tactic(tactic_id: str, dest_dir: Path):
    """
    Baixa a página HTML de uma tática ATT&CK (ex.: TA0043).
    """
    url = f"{MITRE_BASE}/tactics/{tactic_id}/"
    print(f"[MITRE] {tactic_id} -> {url}")
    _download_text(url, dest_dir / f"{tactic_id}.html")


# =========================================================
# 3) Outros sites (Elastic, Splunk, QRadar)
# =========================================================

def collect_simple_html(url: str, dest_dir: Path, filename_prefix: str):
    """
    Coletor simples: baixa apenas a página HTML.
    Útil para docs genéricos de SIEM (Elastic/Splunk/QRadar).
    """
    dest = dest_dir / f"{filename_prefix}.html"
    _download_text(url, dest)


# =========================================================
# 4) Coleta por CAMPO do modelo
# =========================================================

def collect_for_fase_nist():
    """
    fase_nist -> NIST SP 800-61 (rev mais recente, Final)
    """
    print("\n=== fase_nist ===")
    detail = nist_search_detail_url_sp("61")
    if not detail:
        return
    nist_download_pdf_from_detail(detail, FIELD_DIRS["fase_nist"])


def collect_for_status_incidente():
    """
    status_incidente -> Portais de SIEM:
      - Elastic docs index
      - Splunk docs index
      - QRadar docs index
    """
    print("\n=== status_incidente ===")
    dest = FIELD_DIRS["status_incidente"]

    collect_simple_html(
        "https://www.elastic.co/guide/index.html",
        dest,
        "elastic_docs_index",
    )
    collect_simple_html(
        "https://docs.splunk.com/",
        dest,
        "splunk_docs_index",
    )
    collect_simple_html(
        "https://www.ibm.com/docs/en/qsip/7.5",  # Versão específica
        dest,
        "qradar_docs_index",
    )


def collect_for_orientacao_nist():
    """
    orientacao_nist -> NIST CSF (Core 2.0 JSON + página principal)
    """
    print("\n=== orientacao_nist ===")
    dest = FIELD_DIRS["orientacao_nist"]

    # JSON da NIST CSF 2.0 Reference Tool (Core)
    csf_json_url = (
        "https://csrc.nist.gov/extensions/nudp/services/json/csf/download?olirids=all"
    )
    _download_binary(csf_json_url, dest / "csf_core_v2.0.json")

    # Página principal do Cybersecurity Framework
    csf_main_url = "https://www.nist.gov/cyberframework"
    _download_text(csf_main_url, dest / "cyberframework.html")


def collect_for_acao_recomendada():
    """
    acao_recomendada -> NIST IR Guidelines (SP 800-61, mesma fonte de fase_nist)
    """
    print("\n=== acao_recomendada ===")
    detail = nist_search_detail_url_sp("61")
    if not detail:
        return
    nist_download_pdf_from_detail(detail, FIELD_DIRS["acao_recomendada"])


def collect_for_suporte_analista():
    """
    suporte_analista -> MITRE Incident Response (TA0043)
    """
    print("\n=== suporte_analista ===")
    collect_mitre_tactic("TA0043", FIELD_DIRS["suporte_analista"])


def collect_for_iocs_relacionados():
    """
    iocs_relacionados -> ferramentas externas (MISP, OTX, VT).
    Aqui normalmente não faz sentido baixar HTML; mantemos apenas como links
    configurados em outro lugar (dashboard).
    """
    print("\n=== iocs_relacionados ===")
    print("[INFO] iocs_relacionados usa MISP/OTX/VirusTotal apenas como links externos.")
    # Se quiser, você pode salvar um pequeno README.txt com os links:
    dest = FIELD_DIRS["iocs_relacionados"] / "LEIA-ME.txt"
    text = (
        "Ferramentas de IOC utilizadas pelo Xeque-Mate:\n\n"
        "- MISP: https://www.misp-project.org/\n"
        "- OTX:  https://otx.alienvault.com/\n"
        "- VT:   https://www.virustotal.com/\n"
    )
    dest.write_text(text, encoding="utf-8")
    print(f"[OK] Links anotados em {dest}")


def collect_for_gravidade():
    """
    gravidade -> NIST SP 800-30 (Impact Scale / Risk Assessment)
    """
    print("\n=== gravidade ===")
    detail = nist_search_detail_url_sp("30")
    if not detail:
        return
    nist_download_pdf_from_detail(detail, FIELD_DIRS["gravidade"])


def collect_for_resumo_evento_rag():
    """
    resumo_evento_rag -> NIST IR Workflow (SP 800-61, mesma fonte de fase_nist)
    """
    print("\n=== resumo_evento_rag ===")
    detail = nist_search_detail_url_sp("61")
    if not detail:
        return
    nist_download_pdf_from_detail(detail, FIELD_DIRS["resumo_evento_rag"])


def run_all_by_field():
    """
    Roda todos os coletores, organizando os arquivos por campo do modelo.
    """
    collect_for_fase_nist()
    collect_for_status_incidente()
    collect_for_orientacao_nist()
    collect_for_acao_recomendada()
    collect_for_suporte_analista()
    collect_for_iocs_relacionados()
    collect_for_gravidade()
    collect_for_resumo_evento_rag()


# =========================================================
# Entry point
# =========================================================

if __name__ == "__main__":
    run_all_by_field()
