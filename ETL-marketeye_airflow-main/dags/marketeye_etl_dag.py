"""
marketeye_etl_dag.py - Pipeline ETL MarketEye
Version complète avec toutes les fonctions
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import json
import pandas as pd
from pathlib import Path
from datetime import datetime as dt
import re
import logging

# Configuration du logging
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'marketeye-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

# ============================================
# FONCTIONS COMMUNES
# ============================================

def load_json_file(file_path: Path):
    """Charge un fichier JSON en gérant différents formats"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            
            if not content:
                logger.warning(f"Fichier vide: {file_path.name}")
                return []
            
            if content.startswith('['):
                return json.loads(content)
            else:
                # Format ligne par ligne
                data = []
                for line in content.split('\n'):
                    line = line.strip()
                    if line:
                        try:
                            data.append(json.loads(line))
                        except json.JSONDecodeError as e:
                            logger.warning(f"Ligne JSON invalide: {e}")
                            continue
                return data
    except Exception as e:
        logger.error(f"Erreur chargement {file_path.name}: {e}")
        return []

def clean_price(price_str):
    """Nettoie le prix"""
    if not price_str:
        return 0.0
    if isinstance(price_str, (int, float)):
        return float(price_str)
    
    price_clean = re.sub(r'[^\d,.]', '', str(price_str))
    price_clean = price_clean.replace(',', '.')
    numbers = re.findall(r'\d+\.?\d*', price_clean)
    return float(numbers[0]) if numbers else 0.0

def normalize_brand(brand_str):
    """Normalise la marque"""
    if not brand_str:
        return "Unknown"
    
    brand_upper = str(brand_str).upper().strip()
    
    brand_mapping = {
        'APPLE': 'Apple', 'IPHONE': 'Apple',
        'SAMSUNG': 'Samsung',
        'HUAWEI': 'Huawei', 'HONOR': 'Honor',
        'XIAOMI': 'Xiaomi', 'REDMI': 'Xiaomi', 'POCO': 'Xiaomi',
        'OPPO': 'Oppo', 'REALME': 'Realme',
        'NOKIA': 'Nokia', 'TECNO': 'Tecno',
        'INFINIX': 'Infinix', 'VIVO': 'Vivo',
        'MOTOROLA': 'Motorola', 'MOTO': 'Motorola',
        'ONEPLUS': 'OnePlus'
    }
    
    for key, value in brand_mapping.items():
        if key in brand_upper:
            return value
    
    return brand_str.title()

# ============================================
# FONCTIONS AUXILIAIRES AVITO
# ============================================

def extract_brand_avito(item: dict) -> str:
    """Extrait la marque d'un item Avito"""
    brand = item.get('brand')
    
    if brand and str(brand).strip().upper() != 'NULL':
        brand_str = str(brand).upper().strip()
        
        brand_mapping = {
            'APPLE': 'Apple', 'IPHONE': 'Apple',
            'SAMSUNG': 'Samsung',
            'HUAWEI': 'Huawei', 'HONOR': 'Honor',
            'XIAOMI': 'Xiaomi', 'REDMI': 'Xiaomi', 'POCO': 'Xiaomi',
            'OPPO': 'Oppo', 'REALME': 'Realme',
            'NOKIA': 'Nokia', 'TECNO': 'Tecno',
            'INFINIX': 'Infinix', 'VIVO': 'Vivo',
            'MOTOROLA': 'Motorola', 'MOTO': 'Motorola',
            'ONEPLUS': 'OnePlus'
        }
        
        for key, value in brand_mapping.items():
            if key in brand_str:
                return value
        
        return brand_str.title()
    
    # Fallback: extraire depuis le titre
    title = item.get('title', '').upper()
    brands = ['APPLE', 'SAMSUNG', 'HUAWEI', 'XIAOMI', 'OPPO', 'REALME', 'NOKIA']
    
    for brand_name in brands:
        if brand_name in title:
            return brand_name.title()
    
    return "Unknown"

def extract_model_avito(item: dict, brand: str) -> str:
    """Extrait le modèle d'un item Avito"""
    model = item.get('model')
    
    if model and str(model).strip().upper() != 'NULL' and str(model).strip().upper() != 'UNKNOWN':
        return str(model).strip().upper()
    
    # Fallback: extraire depuis le titre
    title = item.get('title', '')
    brand_lower = brand.lower()
    
    # Supprimer la marque du titre
    title_clean = title.lower().replace(brand_lower, '').strip()
    
    # Patterns pour extraire le modèle
    patterns = [
        r'([a-z]+\s*\d+\w*\s*\d*\w*)',  # iPhone 12 Pro, Samsung A14 5G
        r'(\d+\s*[a-z]+\s*\d*)',        # 12 Pro, 14 Plus
        r'([a-z]+\s*\d+)',              # Galaxy S21, Redmi Note 12
        r'(\d+\s*go|\d+\s*gb)',         # 128GB, 256 Go
    ]
    
    for pattern in patterns:
        match = re.search(pattern, title_clean, re.IGNORECASE)
        if match:
            return match.group(1).upper().strip()
    
    return "Unknown"

def extract_price_avito(price_str: str) -> float:
    """Extrait le prix depuis une chaîne comme '250 DH'"""
    return clean_price(price_str)

def extract_specs_avito(item: dict) -> dict:
    """Extrait les spécifications d'un item Avito"""
    specs = {}
    
    # Stockage
    storage = item.get('storage')
    if storage and str(storage).upper() != 'NULL':
        specs['storage'] = str(storage).upper()
    
    # RAM
    ram = item.get('ram')
    if ram and str(ram).upper() != 'NULL':
        specs['ram'] = str(ram).upper()
    
    # Batterie
    battery = item.get('battery_health')
    if battery and str(battery).upper() != 'NULL':
        specs['battery_health'] = str(battery)
    
    # Couleur
    color = item.get('color')
    if color and str(color).upper() != 'NULL':
        specs['color'] = str(color).title()
    
    return specs

def determine_condition_avito(condition: str) -> str:
    """Détermine la condition du produit"""
    if not condition or str(condition).upper() == 'NULL':
        return 'used'
    
    cond_lower = str(condition).lower().strip()
    
    condition_mapping = {
        'neuf': 'new',
        'new': 'new',
        'bon': 'good',
        'good': 'good',
        'excellent': 'excellent',
        'moyen': 'fair',
        'fair': 'fair',
        'mauvais': 'poor',
        'poor': 'poor'
    }
    
    for key, value in condition_mapping.items():
        if key in cond_lower:
            return value
    
    return 'used'

def create_product_id_avito(brand: str, model: str, title: str) -> str:
    """Crée un ID produit unique pour Avito"""
    clean_brand = re.sub(r'[^a-z0-9]', '', brand.lower())
    clean_model = re.sub(r'[^a-z0-9]', '', model.lower())
    
    if clean_model == "unknown":
        title_clean = re.sub(r'[^a-z0-9]', ' ', title.lower())
        words = title_clean.split()
        if len(words) > 1:
            clean_model = words[1]
    
    return f"{clean_brand}_{clean_model}"

# ============================================
# FONCTION AVITO PRINCIPALE
# ============================================

def extract_avito_data(**context):
    """Extrait et transforme les données Avito"""
    logger.info("📥 Extraction des données AVITO")
    
    try:
        raw_dir = Path("/opt/airflow/data/raw")
        processed_dir = Path("/opt/airflow/data/processed")
        processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Chercher les fichiers Avito
        avito_files = []
        for file_path in raw_dir.glob("*"):
            if file_path.is_file():
                filename_lower = file_path.name.lower()
                if any(pattern in filename_lower for pattern in ['avito', 'ads']):
                    avito_files.append(file_path)
        
        if not avito_files:
            logger.warning("⚠️ Aucun fichier Avito trouvé")
            context['ti'].xcom_push(key='avito_count', value=0)
            return 0
        
        all_products = []
        
        for file_path in avito_files:
            logger.info(f"📄 Traitement Avito: {file_path.name}")
            
            data = load_json_file(file_path)
            logger.info(f"✅ Avito: {len(data)} annonces chargées")
            
            # Transformer chaque annonce
            transformed = []
            for item in data:
                product = transform_avito_item(item)
                if product:
                    transformed.append(product)
            
            all_products.extend(transformed)
            logger.info(f"🎯 Avito: {len(transformed)} produits transformés")
        
        # Sauvegarder
        output_path = processed_dir / "avito_transformed.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(all_products, f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Avito sauvegardé: {len(all_products)} produits")
        
        context['ti'].xcom_push(key='avito_count', value=len(all_products))
        context['ti'].xcom_push(key='avito_path', value=str(output_path))
        
        return len(all_products)
        
    except Exception as e:
        logger.error(f"❌ Erreur extraction Avito: {e}")
        raise

def transform_avito_item(item: dict) -> dict:
    """Transforme un item Avito vers le schéma unifié"""
    try:
        # Extraire la marque
        brand = extract_brand_avito(item)
        
        # Extraire le modèle
        model = extract_model_avito(item, brand)
        
        # Extraire le prix
        price = extract_price_avito(item.get('price', '0 DH'))
        
        # Extraire les spécifications
        specs = extract_specs_avito(item)
        
        # Déterminer la condition
        condition = determine_condition_avito(item.get('condition'))
        
        # Créer l'ID produit
        product_id = create_product_id_avito(brand, model, item.get('title', ''))
        
        # Créer le produit
        product = {
            "product_id": product_id,
            "brand": brand,
            "model": model,
            "product_name": item.get('title', '').strip(),
            "specifications": specs,
            "offers": [{
                "source": "Avito",
                "price": price,
                "currency": "MAD",
                "condition": condition,
                "seller_type": item.get('seller_type', 'PRIVATE'),
                "location": {
                    "city": item.get('city', ''),
                    "area": item.get('area', '')
                },
                "url": item.get('url', f"https://www.avito.ma/vi/{item.get('ad_id', '')}.htm"),
                "seller_name": item.get('seller_name', ''),
                "scraped_at": item.get('list_time', dt.now().isoformat())
            }],
            "metadata": {
                "sources": ["Avito"],
                "created_at": dt.now().isoformat(),
                "last_updated": dt.now().isoformat()
            }
        }
        
        return product
        
    except Exception as e:
        logger.warning(f"⚠️ Erreur transformation item Avito: {e}")
        return None

# ============================================
# FONCTION JUMIA PRINCIPALE
# ============================================

def extract_jumia_data(**context):
    """Extrait les données Jumia depuis le vrai fichier"""
    logger.info("📥 Extraction des données JUMIA")
    
    try:
        raw_dir = Path("/opt/airflow/data/raw")
        processed_dir = Path("/opt/airflow/data/processed")
        processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Chercher les fichiers Jumia
        jumia_files = []
        for file_path in raw_dir.glob("*"):
            if file_path.is_file():
                filename_lower = file_path.name.lower()
                if any(pattern in filename_lower for pattern in ['jumia', 'jm']):
                    jumia_files.append(file_path)
        
        if not jumia_files:
            logger.warning("⚠️ Aucun fichier Jumia trouvé")
            context['ti'].xcom_push(key='jumia_count', value=0)
            return 0
        
        all_products = []
        
        for file_path in jumia_files:
            logger.info(f"📄 Traitement Jumia: {file_path.name}")
            
            # Charger les données
            data = load_json_file(file_path)
            logger.info(f"✅ Jumia: {len(data)} produits chargés")
            
            # Transformer chaque produit
            transformed = []
            for item in data:
                product = transform_jumia_item(item)
                if product:
                    transformed.append(product)
            
            all_products.extend(transformed)
            logger.info(f"🎯 Jumia: {len(transformed)} produits transformés")
        
        # Sauvegarder
        output_path = processed_dir / "jumia_transformed.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(all_products, f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Jumia sauvegardé: {len(all_products)} produits")
        
        context['ti'].xcom_push(key='jumia_count', value=len(all_products))
        context['ti'].xcom_push(key='jumia_path', value=str(output_path))
        
        return len(all_products)
        
    except Exception as e:
        logger.error(f"❌ Erreur extraction Jumia: {e}")
        raise

def transform_jumia_item(item: dict) -> dict:
    """Transforme un produit Jumia"""
    try:
        brand = normalize_brand(item.get('brand'))
        
        # Extraire modèle depuis le titre
        title = item.get('title', '')
        model = "Unknown"
        if title:
            title_lower = title.lower()
            brand_lower = brand.lower()
            title_clean = title_lower.replace(brand_lower, '').strip()
            
            patterns = [
                r'([a-z]+\s*\d+\w*\s*\d*\w*)',  # galaxy s21 ultra
                r'(\d+\s*[a-z]+\s*\d*)',        # 12 pro max
                r'([a-z]+\s*\d+)',              # note 12
            ]
            
            for pattern in patterns:
                match = re.search(pattern, title_clean, re.IGNORECASE)
                if match:
                    model = match.group(1).upper().strip()
                    break
        
        # Créer ID produit
        clean_brand = re.sub(r'[^a-z0-9]', '', brand.lower())
        clean_model = re.sub(r'[^a-z0-9]', '', model.lower())
        product_id = f"{clean_brand}_{clean_model}"
        
        # Extraire spécifications
        specs = {}
        if item.get('specs'):
            for key, value in item['specs'].items():
                key_str = str(key).lower()
                if 'ram' in key_str and value:
                    specs['ram'] = str(value)
                elif 'stockage' in key_str or 'storage' in key_str and value:
                    specs['storage'] = str(value)
        
        # Créer le produit
        product = {
            "product_id": product_id,
            "brand": brand,
            "model": model,
            "product_name": title.strip(),
            "specifications": specs,
            "offers": [{
                "source": "Jumia",
                "price": clean_price(item.get('price')),
                "currency": "MAD",
                "condition": "new",
                "rating": extract_jumia_rating(item.get('rating')),
                "reviews_count": item.get('reviews_count_text'),
                "url": item.get('product_url'),
                "scraped_at": item.get('scraped_at', dt.now().isoformat())
            }],
            "metadata": {
                "sources": ["Jumia"],
                "created_at": dt.now().isoformat(),
                "last_updated": dt.now().isoformat()
            }
        }
        
        return product
        
    except Exception as e:
        logger.warning(f"⚠️ Erreur transformation Jumia: {e}")
        return None

def extract_jumia_rating(rating_data):
    """Extrait la note Jumia"""
    if not rating_data:
        return 0.0
    if isinstance(rating_data, (int, float)):
        return float(rating_data)
    
    rating_str = str(rating_data)
    match = re.search(r'(\d+\.?\d*)', rating_str)
    return float(match.group(1)) if match else 0.0

# ============================================
# FONCTION ELECTROPLANET PRINCIPALE
# ============================================

def extract_electroplanet_data(**context):
    """Extrait les données Electroplanet depuis le vrai fichier"""
    logger.info("📥 Extraction des données ELECTROPLANET")
    
    try:
        raw_dir = Path("/opt/airflow/data/raw")
        processed_dir = Path("/opt/airflow/data/processed")
        processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Chercher les fichiers Electroplanet
        electro_files = []
        for file_path in raw_dir.glob("*"):
            if file_path.is_file():
                filename_lower = file_path.name.lower()
                if any(pattern in filename_lower for pattern in ['electroplanet', 'electro', 'planet']):
                    electro_files.append(file_path)
        
        if not electro_files:
            logger.warning("⚠️ Aucun fichier Electroplanet trouvé")
            context['ti'].xcom_push(key='electroplanet_count', value=0)
            return 0
        
        all_products = []
        
        for file_path in electro_files:
            logger.info(f"📄 Traitement Electroplanet: {file_path.name}")
            
            # Charger les données
            data = load_json_file(file_path)
            logger.info(f"✅ Electroplanet: {len(data)} produits chargés")
            
            # Transformer chaque produit
            transformed = []
            for item in data:
                product = transform_electroplanet_item(item)
                if product:
                    transformed.append(product)
            
            all_products.extend(transformed)
            logger.info(f"🎯 Electroplanet: {len(transformed)} produits transformés")
        
        # Sauvegarder
        output_path = processed_dir / "electroplanet_transformed.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(all_products, f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Electroplanet sauvegardé: {len(all_products)} produits")
        
        context['ti'].xcom_push(key='electroplanet_count', value=len(all_products))
        context['ti'].xcom_push(key='electroplanet_path', value=str(output_path))
        
        return len(all_products)
        
    except Exception as e:
        logger.error(f"❌ Erreur extraction Electroplanet: {e}")
        raise

def transform_electroplanet_item(item: dict) -> dict:
    """Transforme un produit Electroplanet"""
    try:
        brand = normalize_brand(item.get('brand'))
        
        # Extraire modèle
        model = item.get('specifications', {}).get('Modèle') or "Unknown"
        if model == "Unknown":
            name = item.get('name', '')
            name_clean = name.lower().replace(brand.lower(), '').strip()
            match = re.search(r'([a-z]+\s*\d+\w*)', name_clean)
            if match:
                model = match.group(1).upper()
        
        # Créer ID produit
        clean_brand = re.sub(r'[^a-z0-9]', '', brand.lower())
        clean_model = re.sub(r'[^a-z0-9]', '', model.lower())
        product_id = f"{clean_brand}_{clean_model}"
        
        # Extraire spécifications
        specs = {}
        product_specs = item.get('specifications', {})
        spec_mapping = {
            'Capacité de stockage interne': 'storage',
            'Capacité de la RAM': 'ram',
            'Modèle': 'model'
        }
        
        for key, value in product_specs.items():
            if key in spec_mapping and value:
                specs[spec_mapping[key]] = str(value)
        
        # Créer le produit
        product = {
            "product_id": product_id,
            "brand": brand,
            "model": model,
            "product_name": item.get('name', '').strip(),
            "specifications": specs,
            "offers": [{
                "source": "Electroplanet",
                "price": clean_price(item.get('price')),
                "currency": "MAD",
                "condition": "new",
                "rating": item.get('reviews_summary', {}).get('average_rating'),
                "reviews_count": item.get('reviews_summary', {}).get('total_reviews'),
                "url": item.get('product_url'),
                "scraped_at": item.get('detailed_scraped_at') or item.get('scraped_at', dt.now().isoformat())
            }],
            "metadata": {
                "sources": ["Electroplanet"],
                "created_at": dt.now().isoformat(),
                "last_updated": dt.now().isoformat()
            }
        }
        
        return product
        
    except Exception as e:
        logger.warning(f"⚠️ Erreur transformation Electroplanet: {e}")
        return None

# ============================================
# FONCTIONS DE FUSION ET STATISTIQUES
# ============================================

def merge_data(**context):
    """Fusionne les données de toutes les sources"""
    logger.info("🔄 Fusion et déduplication des données")
    
    try:
        processed_dir = Path("/opt/airflow/data/processed")
        all_products = []
        
        # Charger toutes les sources
        sources = ['avito', 'jumia', 'electroplanet']
        
        for source in sources:
            file_path = processed_dir / f"{source}_transformed.json"
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_products.extend(data)
                logger.info(f"📁 {source}: {len(data)} produits chargés")
            else:
                logger.warning(f"⚠️ Fichier {source} non trouvé")
        
        if not all_products:
            logger.warning("⚠️ Aucune donnée à fusionner")
            context['ti'].xcom_push(key='total_products', value=0)
            return 0
        
        # Normaliser les IDs pour une meilleure fusion
        for product in all_products:
            # Standardiser l'ID (minuscules, pas d'espaces)
            original_id = product.get('product_id', '')
            if original_id:
                clean_id = original_id.lower().replace(' ', '_')
                product['product_id'] = clean_id
        
        # Fusionner les produits par ID
        merged_dict = {}
        
        for product in all_products:
            pid = product.get('product_id')
            if not pid:
                logger.warning("Produit sans ID, ignoré")
                continue
            
            if pid in merged_dict:
                # Produit existant, fusionner
                existing = merged_dict[pid]
                
                # 1. Fusionner les offres
                if 'offers' in product:
                    if 'offers' not in existing:
                        existing['offers'] = []
                    
                    # Ajouter seulement les offres uniques
                    for new_offer in product['offers']:
                        # Vérifier si cette offre existe déjà
                        offer_exists = False
                        for existing_offer in existing['offers']:
                            # Comparer par source et URL
                            if (new_offer.get('source') == existing_offer.get('source') and
                                new_offer.get('url') == existing_offer.get('url')):
                                offer_exists = True
                                break
                        
                        if not offer_exists:
                            existing['offers'].append(new_offer)
                
                # 2. Fusionner les spécifications
                if 'specifications' in product:
                    if 'specifications' not in existing:
                        existing['specifications'] = {}
                    
                    for key, value in product['specifications'].items():
                        if key not in existing['specifications'] or not existing['specifications'][key]:
                            existing['specifications'][key] = value
                
                # 3. Mettre à jour les métadonnées
                if 'metadata' in product:
                    if 'metadata' not in existing:
                        existing['metadata'] = {'sources': []}
                    
                    # Ajouter la source si elle n'existe pas
                    if 'sources' in product['metadata']:
                        for source in product['metadata']['sources']:
                            if source not in existing['metadata'].get('sources', []):
                                existing['metadata'].setdefault('sources', []).append(source)
                    
                    # Mettre à jour last_updated
                    existing['metadata']['last_updated'] = dt.now().isoformat()
                
                # 4. Garder le meilleur nom de produit (le plus long/descriptif)
                if ('product_name' in product and 
                    len(product.get('product_name', '')) > len(existing.get('product_name', ''))):
                    existing['product_name'] = product['product_name']
                    
            else:
                # Nouveau produit
                merged_dict[pid] = product
        
        final_products = list(merged_dict.values())
        
        # Compter les offres par source
        source_counts = {}
        for product in final_products:
            for offer in product.get('offers', []):
                source = offer.get('source', 'Unknown')
                source_counts[source] = source_counts.get(source, 0) + 1
        
        # Sauvegarder
        output_path = processed_dir / "marketeye_final.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(final_products, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✅ Fusion terminée: {len(final_products)} produits uniques")
        logger.info(f"📊 Offres par source: {source_counts}")
        
        context['ti'].xcom_push(key='total_products', value=len(final_products))
        context['ti'].xcom_push(key='final_data_path', value=str(output_path))
        context['ti'].xcom_push(key='source_counts', value=source_counts)
        
        return len(final_products)
        
    except Exception as e:
        logger.error(f"❌ Erreur fusion: {e}")
        raise
    
def calculate_statistics(**context):
    """Calcule les statistiques"""
    logger.info("📊 Calcul des statistiques")
    
    try:
        final_path = context['ti'].xcom_pull(key='final_data_path', task_ids='merge_data')
        
        if final_path and Path(final_path).exists():
            with open(final_path, 'r', encoding='utf-8') as f:
                products = json.load(f)
            
            # Calculs simples
            total = len(products)
            prices = []
            for product in products:
                for offer in product.get('offers', []):
                    price = offer.get('price', 0)
                    if price > 0:
                        prices.append(price)
            
            stats = {
                "total_products": total,
                "total_offers": sum(len(p.get('offers', [])) for p in products),
                "avg_price": sum(prices) / len(prices) if prices else 0,
                "min_price": min(prices) if prices else 0,
                "max_price": max(prices) if prices else 0,
                "sources": list(set(
                    offer.get('source', 'Unknown') 
                    for p in products 
                    for offer in p.get('offers', [])
                ))
            }
            
            # Sauvegarder les stats
            stats_path = Path("/opt/airflow/data/processed") / "statistics.json"
            with open(stats_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=2)
            
            logger.info(f"📈 Statistiques: {stats}")
            
            context['ti'].xcom_push(key='statistics', value=stats)
            return stats
            
        else:
            logger.warning("⚠️ Aucune donnée à analyser")
            return {"error": "No data available"}
            
    except Exception as e:
        logger.error(f"❌ Erreur calcul stats: {e}")
        raise

def generate_report(**context):
    """Génère un rapport final"""
    logger.info("📄 Génération du rapport")
    
    try:
        stats = context['ti'].xcom_pull(key='statistics', task_ids='calculate_statistics')
        
        if stats and 'error' not in stats:
            report = f"""
            ===========================================
            RAPPORT ETL MARKETEYE - {dt.now().strftime('%Y-%m-%d %H:%M')}
            ===========================================
            
            📊 RÉSUMÉ:
            - Produits uniques: {stats.get('total_products', 0)}
            - Offres totales: {stats.get('total_offers', 0)}
            - Prix moyen: {stats.get('avg_price', 0):.2f} MAD
            - Prix min: {stats.get('min_price', 0):.2f} MAD
            - Prix max: {stats.get('max_price', 0):.2f} MAD
            
            🌐 SOURCES: {', '.join(stats.get('sources', []))}
            
            ✅ Pipeline exécuté avec succès!
            """
        else:
            report = "⚠️ Rapport: Aucune donnée disponible ou erreur dans le pipeline"
        
        # Sauvegarder le rapport
        report_path = Path("/opt/airflow/data/processed") / f"report_{dt.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"💾 Rapport sauvegardé: {report_path}")
        
        return report
        
    except Exception as e:
        logger.error(f"❌ Erreur génération rapport: {e}")
        raise

# ============================================
# FONCTIONS DE STOCKAGE - À AJOUTER
# ============================================

def save_to_postgresql(**context):
    """Stocke les données finales dans PostgreSQL"""
    logger.info("🗄️ Stockage dans PostgreSQL")
    
    try:
        from sqlalchemy import create_engine, text
        import pandas as pd
        
        # Charger les données finales
        final_path = Path("/opt/airflow/data/processed/marketeye_final.json")
        if not final_path.exists():
            logger.error("❌ Fichier final non trouvé")
            return 0
        
        with open(final_path, 'r', encoding='utf-8') as f:
            products = json.load(f)
        
        # Connexion à PostgreSQL (même que Airflow)
        engine = create_engine('postgresql://airflow:airflow@postgres/marketeye_db')
        
        # Créer la base de données si elle n'existe pas
        with engine.connect() as conn:
            conn.execute(text("COMMIT"))
            try:
                conn.execute(text("CREATE DATABASE marketeye_db"))
                logger.info("✅ Base de données marketeye_db créée")
            except:
                logger.info("ℹ️ Base de données marketeye_db existe déjà")
        
        # Reconnecter à la nouvelle base
        engine = create_engine('postgresql://airflow:airflow@postgres/marketeye_db')
        
        # Préparer les données pour PostgreSQL
        products_data = []
        offers_data = []
        
        for product in products:
            # Données produit
            product_row = {
                'product_id': product.get('product_id'),
                'brand': product.get('brand'),
                'model': product.get('model'),
                'product_name': product.get('product_name'),
                'specifications': json.dumps(product.get('specifications', {})),
                'created_at': dt.now(),
                'updated_at': dt.now()
            }
            products_data.append(product_row)
            
            # Données offres
            for offer in product.get('offers', []):
                offer_row = {
                    'product_id': product.get('product_id'),
                    'source': offer.get('source'),
                    'price': offer.get('price'),
                    'currency': offer.get('currency', 'MAD'),
                    'condition': offer.get('condition'),
                    'seller_type': offer.get('seller_type'),
                    'url': offer.get('url'),
                    'scraped_at': offer.get('scraped_at')
                }
                offers_data.append(offer_row)
        
        # Convertir en DataFrames
        df_products = pd.DataFrame(products_data)
        df_offers = pd.DataFrame(offers_data)
        
        # Sauvegarder dans PostgreSQL
        df_products.to_sql('products', engine, if_exists='replace', index=False)
        df_offers.to_sql('offers', engine, if_exists='replace', index=False)
        
        # Créer des index
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_products_product_id ON products(product_id);
                CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);
                CREATE INDEX IF NOT EXISTS idx_offers_product_id ON offers(product_id);
                CREATE INDEX IF NOT EXISTS idx_offers_source ON offers(source);
                CREATE INDEX IF NOT EXISTS idx_offers_price ON offers(price);
            """))
        
        logger.info(f"✅ PostgreSQL: {len(df_products)} produits, {len(df_offers)} offres")
        
        context['ti'].xcom_push(key='postgres_stats', 
                               value={'products': len(df_products), 'offers': len(df_offers)})
        return len(df_products)
        
    except Exception as e:
        logger.error(f"❌ Erreur PostgreSQL: {e}")
        raise

def save_to_mongodb(**context):
    """Stocke les données finales dans MongoDB"""
    logger.info("🗄️ Stockage dans MongoDB")
    
    try:
        from pymongo import MongoClient
        
        # Charger les données finales
        final_path = Path("/opt/airflow/data/processed/marketeye_final.json")
        if not final_path.exists():
            logger.error("❌ Fichier final non trouvé")
            return 0
        
        with open(final_path, 'r', encoding='utf-8') as f:
            products = json.load(f)
        
        # Connexion à MongoDB
        client = MongoClient(
            host="mongodb",  # Nom du service dans docker-compose
            port=27017,
            username="admin",
            password="password",
            authSource="admin"
        )
        
        # Utiliser la base de données marketeye
        db = client.marketeye
        
        # Vider les collections existantes (optionnel)
        db.products.delete_many({})
        db.offers.delete_many({})
        
        # Insérer les produits
        if products:
            result = db.products.insert_many(products)
            logger.info(f"✅ MongoDB: {len(result.inserted_ids)} produits insérés")
            
            # Créer des index
            db.products.create_index([("product_id", 1)], unique=True)
            db.products.create_index([("brand", 1)])
            db.products.create_index([("price", 1)])
            
            context['ti'].xcom_push(key='mongodb_stats', 
                                   value={'products': len(result.inserted_ids)})
            return len(result.inserted_ids)
        else:
            logger.warning("⚠️ Aucun produit à insérer dans MongoDB")
            return 0
        
    except Exception as e:
        logger.error(f"❌ Erreur MongoDB: {e}")
        raise

def save_to_json_backup(**context):
    """Sauvegarde supplémentaire dans JSON formaté"""
    logger.info("💾 Sauvegarde JSON de backup")
    
    try:
        final_path = Path("/opt/airflow/data/processed/marketeye_final.json")
        if not final_path.exists():
            logger.error("❌ Fichier final non trouvé")
            return 0
        
        with open(final_path, 'r', encoding='utf-8') as f:
            products = json.load(f)
        
        # Sauvegarder avec timestamp
        backup_dir = Path("/opt/airflow/data/backups")
        backup_dir.mkdir(exist_ok=True)
        
        timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"marketeye_backup_{timestamp}.json"
        
        with open(backup_path, 'w', encoding='utf-8') as f:
            json.dump(products, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✅ Backup JSON: {backup_path.name}")
        return len(products)
        
    except Exception as e:
        logger.error(f"❌ Erreur backup JSON: {e}")
        raise
    
# ============================================
# DAG PRINCIPAL
# ============================================

with DAG(
    'marketeye_etl',
    default_args=default_args,
    description='ETL MarketEye - Lecture des vrais fichiers',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'marketeye', 'production']
) as dag:

    # Tâches
    start = DummyOperator(task_id='start')
    
    extract_jumia = PythonOperator(
        task_id='extract_jumia_data',
        python_callable=extract_jumia_data,
        provide_context=True
    )
    
    extract_avito = PythonOperator(
        task_id='extract_avito_data',
        python_callable=extract_avito_data,
        provide_context=True
    )
    
    extract_electroplanet = PythonOperator(
        task_id='extract_electroplanet_data',
        python_callable=extract_electroplanet_data,
        provide_context=True
    )
    
    merge = PythonOperator(
        task_id='merge_data',
        python_callable=merge_data,
        provide_context=True
    )
    
    stats = PythonOperator(
        task_id='calculate_statistics',
        python_callable=calculate_statistics,
        provide_context=True
    )
    
    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True
    )
    
    # AJOUTEZ CES NOUVELLES TÂCHES :
    save_postgres = PythonOperator(
        task_id='save_to_postgresql',
        python_callable=save_to_postgresql,
        provide_context=True,
        execution_timeout=timedelta(minutes=10)
    )
    
    save_mongo = PythonOperator(
        task_id='save_to_mongodb',
        python_callable=save_to_mongodb,
        provide_context=True,
        execution_timeout=timedelta(minutes=5)
    )

    save_backup = PythonOperator(
        task_id='save_to_json_backup',
        python_callable=save_to_json_backup,
        provide_context=True
    )

    end = DummyOperator(task_id='end')
    
    # Orchestration
    start >> [extract_jumia, extract_avito, extract_electroplanet] >> merge >> stats >> report
    report >> [save_postgres, save_mongo, save_backup] >> end