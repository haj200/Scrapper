import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

FILENAME = "donnees_marches.json"
TODAY = datetime.today().strftime("%d/%m/%Y")

# Charger les anciennes données
if os.path.exists(FILENAME):
    with open(FILENAME, "r", encoding="utf-8") as f:
        all_data = json.load(f)
else:
    all_data = []

existing_refs = set(item["reference"] for item in all_data if item.get("reference"))

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

new_data = []

def process_page(page):
    url = f"https://www.marchespublics.gov.ma/bdc/entreprise/consultation/resultat?page={page}"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        cards = soup.select('.entreprise__card')

        page_data = []

        for card in cards:
            try:
                ref = card.select_one('.font-bold.table__links')
                ref_text = ref.text.strip().replace('Référence :', '').strip() if ref else None
                if ref_text in existing_refs:
                    continue

                objet_div = card.select_one('[data-bs-toggle="tooltip"]')
                objet_text = objet_div.text.strip().replace('Objet :', '').strip() if objet_div else None

                acheteur_span = card.find('span', string=lambda s: s and "Acheteur" in s)
                acheteur = acheteur_span.parent.text.replace('Acheteur :', '').strip() if acheteur_span else None

                date_span = card.find('span', string=lambda s: s and "Date de publication" in s)
                date_pub_full = date_span.parent.text.replace('Date de publication du résultat :', '').strip() if date_span else None
                date_pub = date_pub_full.split(" ")[0] if date_pub_full else None

                if date_pub != TODAY:
                    continue

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

                page_data.append({
                    "reference": ref_text,
                    "objet": objet_text,
                    "acheteur": acheteur,
                    "date_publication": date_pub_full,
                    "nombre_devis": nombre_devis,
                    "attribue": attribue,
                    "entreprise_attributaire": entreprise_attributaire if attribue else None,
                    "montant": montant_ttc if attribue else None
                })

            except Exception:
                continue

        return page_data

    except Exception:
        return []

# Traitement parallèle
max_pages = 100
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(process_page, page): page for page in range(1, max_pages + 1)}
    for future in tqdm(as_completed(futures), total=max_pages):
        data = future.result()
        if data:
            new_data.extend(data)

# Sauvegarde
if new_data:
    all_data.extend(new_data)
    with open(FILENAME, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    print(f"✅ {len(new_data)} nouvelles données ajoutées pour {TODAY}.")
else:
    print("ℹ️ Aucune donnée nouvelle trouvée pour aujourd'hui.")
