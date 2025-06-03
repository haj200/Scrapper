import requests
from bs4 import BeautifulSoup
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

MAX_PAGE = 17125
MAX_WORKERS = 10        # Ne PAS augmenter plus de 15 pour éviter blocage IP
RETRIES = 3
TIMEOUT = 10

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

session = requests.Session()
session.headers.update(headers)

all_data = []
failed_pages = []

def extract_card_data(card):
    try:
        ref = card.select_one('.font-bold.table__links')
        ref_text = ref.text.strip().replace('Référence :', '').strip() if ref else None

        objet_div = card.select_one('[data-bs-toggle="tooltip"]')
        objet_text = objet_div.text.strip().replace('Objet :', '').strip() if objet_div else None

        acheteur_span = card.find('span', string=lambda s: s and "Acheteur" in s)
        acheteur = acheteur_span.parent.text.replace('Acheteur :', '').strip() if acheteur_span else None

        date_span = card.find('span', string=lambda s: s and "Date de publication" in s)
        date_pub = date_span.parent.text.replace('Date de publication du résultat :', '').strip() if date_span else None

        right_card = card.select_one('.entreprise__rightSubCard--top')
        nombre_devis = entreprise_attributaire = montant_ttc = None
        attribue = False

        if right_card:
            devis_match = right_card.find(string=lambda s: s and "Nombre de devis reçus" in s)
            if devis_match:
                devis_span = right_card.select_one("span span.font-bold")
                if devis_span:
                    nombre_devis = devis_span.text.strip()

            spans = right_card.find_all('span', recursive=False)

            def get_bold_text(span):
                bold = span.find('span', class_='font-bold')
                return bold.text.strip() if bold else None

            if len(spans) >= 3:
                entreprise_attributaire = get_bold_text(spans[1])
                montant_ttc = get_bold_text(spans[2])
                attribue = entreprise_attributaire is not None

        return {
            "reference": ref_text,
            "objet": objet_text,
            "acheteur": acheteur,
            "date_publication": date_pub,
            "nombre_devis": nombre_devis,
            "attribue": attribue,
            "entreprise_attributaire": entreprise_attributaire if attribue else None,
            "montant": montant_ttc if attribue else None
        }

    except Exception as e:
        print(f"Erreur lors de l'extraction d'une carte: {e}")
        return None

def fetch_page(page):
    url = f"https://www.marchespublics.gov.ma/bdc/entreprise/consultation/resultat?page={page}"

    for attempt in range(RETRIES):
        try:
            time.sleep(random.uniform(0.4, 1.2))  # Pause pour limiter charge serveur
            response = session.get(url, timeout=TIMEOUT)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml')
                cards = soup.select('.entreprise__card')
                return [extract_card_data(card) for card in cards if card]
            else:
                print(f"[{page}] ❌ Statut HTTP {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"[{page}] ⚠️ Tentative {attempt+1}/{RETRIES} échouée : {e}")
            time.sleep(1 + random.uniform(0, 1))
    failed_pages.append(page)
    return []

# Extraction multi-thread
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(fetch_page, page): page for page in range(1, MAX_PAGE + 1)}

    for future in tqdm(as_completed(futures), total=len(futures), desc="Extraction en cours"):
        result = future.result()
        if result:
            all_data.extend(result)

# Nettoyage : enlever les enregistrements None
all_data = [d for d in all_data if d is not None]

# Sauvegarde JSON
with open("donnees_marches.json", "w", encoding='utf-8') as f:
    json.dump(all_data, f, ensure_ascii=False, indent=2)

# Sauvegarde des pages échouées
if failed_pages:
    with open("pages_non_traitees.json", "w") as f:
        json.dump(failed_pages, f)
    print(f"⚠️ {len(failed_pages)} pages ont échoué. Liste enregistrée dans pages_non_traitees.json")

print(f"✅ Extraction terminée. {len(all_data)} enregistrements sauvegardés.")
