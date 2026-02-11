# scripts/data_processors/avito_extractor.py
import json
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from .base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

class AvitoExtractor(BaseExtractor):
    """Extracteur spécialisé pour Avito - STRUCTURE 2024"""
    
    def extract(self, file_path: Path) -> List[Dict]:
        """Extrait les données Avito depuis un fichier"""
        return self.load_json_file(file_path)
    
    def transform(self, raw_product: Dict) -> Dict:
        """Transforme une annonce Avito - VERSION CORRIGÉE"""
        try:
            # DEBUG: Afficher les données reçues
            logger.debug(f"Données Avito reçues: {raw_product.get('title', 'Titre manquant')}")
            
            # 1. EXTRACTION MARQUE (PRIORITÉ MAX)
            brand = self._extract_brand_fixed(raw_product)
            
            # 2. EXTRACTION MODÈLE
            model = self._extract_model_fixed(raw_product, brand)
            
            # 3. NETTOYAGE PRIX
            price = self._extract_price_fixed(raw_product)
            
            # 4. SPÉCIFICATIONS
            specs = self._extract_specs_fixed(raw_product)
            
            # 5. ID PRODUIT (FIXÉ)
            product_id = self._create_product_id_fixed(brand, model, raw_product)
            
            # 6. CONDITION
            condition = self._determine_condition_fixed(raw_product)
            
            # 7. URL CORRECTE
            url = self._build_url_fixed(raw_product)
            
            # 8. CRÉATION OFFRE
            offer = {
                "source": "Avito",
                "price": price,
                "currency": "MAD",
                "condition": condition,
                "seller_type": raw_product.get('seller_type', 'PRIVATE'),
                "location": {
                    "city": raw_product.get('city', ''),
                    "area": raw_product.get('area', '')
                },
                "url": url,
                "seller_name": raw_product.get('seller_name', ''),
                "scraped_at": raw_product.get('list_time', datetime.now().isoformat())
            }
            
            # 9. PRODUIT MAÎTRE
            master_product = self.master_schema.copy()
            master_product.update({
                "product_id": product_id,
                "brand": brand,
                "model": model,
                "product_name": raw_product.get('title', '').strip(),
                "specifications": specs,
                "offers": [offer],
                "metadata": {
                    "sources": ["Avito"],
                    "created_at": datetime.now().isoformat(),
                    "last_updated": datetime.now().isoformat()
                }
            })
            
            logger.info(f"✅ Avito transformé: {brand} {model} - {price} MAD")
            return master_product
            
        except Exception as e:
            logger.error(f"❌ Erreur transformation Avito: {e}")
            logger.error(f"Données problématiques: {raw_product.get('title', 'Pas de titre')}")
            return None
    
    # ============================================
    # MÉTHODES FIXÉES
    # ============================================
    
    def _extract_brand_fixed(self, product: Dict) -> str:
        """Extrait la marque de manière ROBUSTE"""
        # 1. Depuis le champ 'brand'
        brand_field = product.get('brand')
        if brand_field and str(brand_field).strip().upper() not in ['', 'NULL', 'NONE', 'INCONNU']:
            brand = str(brand_field).strip().upper()
            
            # Mapping des marques
            brand_mapping = {
                'APPLE': 'Apple', 'IPHONE': 'Apple',
                'SAMSUNG': 'Samsung', 'SAMSG': 'Samsung',
                'XIAOMI': 'Xiaomi', 'REDMI': 'Xiaomi', 'POCO': 'Xiaomi',
                'HUAWEI': 'Huawei', 'HONOR': 'Huawei',
                'OPPO': 'Oppo', 'REALME': 'Realme',
                'NOKIA': 'Nokia', 'TECNO': 'Tecno',
                'INFINIX': 'Infinix', 'VIVO': 'Vivo',
                'MOTOROLA': 'Motorola', 'MOTO': 'Motorola',
                'ONEPLUS': 'OnePlus', 'SONY': 'Sony',
                'LG': 'LG', 'GOOGLE': 'Google', 'PIXEL': 'Google'
            }
            
            for key, value in brand_mapping.items():
                if key in brand:
                    return value
            
            return brand.title()
        
        # 2. Depuis le titre (fallback)
        title = product.get('title', '').upper()
        
        # Liste exhaustive des marques
        brands_in_title = [
            ('APPLE', 'Apple'), ('IPHONE', 'Apple'),
            ('SAMSUNG', 'Samsung'), ('GALAXY', 'Samsung'),
            ('XIAOMI', 'Xiaomi'), ('REDMI', 'Xiaomi'), ('POCO', 'Xiaomi'),
            ('HUAWEI', 'Huawei'), ('HONOR', 'Huawei'),
            ('OPPO', 'Oppo'), ('REALME', 'Realme'),
            ('NOKIA', 'Nokia'), ('TECNO', 'Tecno'),
            ('INFINIX', 'Infinix'), ('VIVO', 'Vivo'),
            ('MOTOROLA', 'Motorola'), ('MOTO', 'Motorola'),
            ('ONEPLUS', 'OnePlus'), ('SONY', 'Sony'),
            ('LG', 'LG'), ('GOOGLE', 'Google'), ('PIXEL', 'Google')
        ]
        
        for brand_key, brand_value in brands_in_title:
            if brand_key in title:
                return brand_value
        
        # 3. Depuis le modèle
        model_field = product.get('model', '')
        if model_field:
            for brand_key, brand_value in brands_in_title:
                if brand_key in str(model_field).upper():
                    return brand_value
        
        return "Unknown"
    
    def _extract_model_fixed(self, product: Dict, brand: str) -> str:
        """Extrait le modèle de manière ROBUSTE"""
        # 1. Depuis le champ 'model'
        model_field = product.get('model')
        if model_field and str(model_field).strip().upper() not in ['', 'NULL', 'NONE', 'UNKNOWN']:
            model = str(model_field).strip().upper()
            # Nettoyer le modèle
            model = re.sub(r'[^\w\s]', ' ', model)  # Garder lettres, chiffres, espaces
            model = re.sub(r'\s+', ' ', model).strip()
            return model if model else "Unknown"
        
        # 2. Depuis le titre (extraction intelligente)
        title = product.get('title', '').upper()
        
        # Supprimer la marque du titre
        brand_lower = brand.lower()
        if brand != "Unknown":
            title = title.replace(brand.upper(), "")
        
        # Patterns pour extraire le modèle
        patterns = [
            r'([A-Z]+\s*\d+\s*[A-Z]*\s*\d*\s*[A-Z]*)',  # S24 ULTRA, 12T PRO
            r'(\d+\s*[A-Z]+\s*\d*)',                    # 12 PRO, 14 PLUS
            r'([A-Z]+\s*\d+)',                         # GALAXY S21, REDMI NOTE 12
            r'(\d+\s*[A-Z]{2,})',                      # 256GB, 512 GO
            r'([A-Z]{2,}\s*\d+)',                      # NOTE 10, TAB S9
        ]
        
        for pattern in patterns:
            match = re.search(pattern, title)
            if match:
                model_found = match.group(1).strip()
                # Nettoyer
                model_found = re.sub(r'\b(ULTRA|PRO|PLUS|MAX|MINI|LITE)\b', '', model_found, flags=re.IGNORECASE)
                model_found = re.sub(r'\s+', ' ', model_found).strip().upper()
                if model_found and len(model_found) > 1:
                    return model_found
        
        # 3. Prendre les premiers mots significatifs du titre
        words = title.split()
        meaningful_words = []
        for word in words[:3]:  # Prendre max 3 premiers mots
            if len(word) > 2 and not word.isdigit():  # Éviter mots courts et chiffres seuls
                meaningful_words.append(word)
        
        if meaningful_words:
            return ' '.join(meaningful_words).upper()
        
        return "Unknown"
    
    def _extract_price_fixed(self, product: Dict) -> float:
        """Extrait le prix - robuste aux formats différents"""
        price_data = product.get('price')
        
        if price_data is None:
            return 0.0
        
        # Si c'est déjà un nombre
        if isinstance(price_data, (int, float)):
            return float(price_data)
        
        # Convertir en string
        price_str = str(price_data)
        
        # Formats supportés: "250 DH", "1,200.50 MAD", "3500", "4.500,00"
        price_clean = re.sub(r'[^\d,.]', '', price_str)
        
        # Gérer format européen 4.500,00 → 4500.00
        if ',' in price_clean and '.' in price_clean:
            # Format: 1.200,50
            price_clean = price_clean.replace('.', '')
            price_clean = price_clean.replace(',', '.')
        elif ',' in price_clean:
            # Format: 4,500 → 4500
            price_clean = price_clean.replace(',', '')
        
        # Extraire le premier nombre
        numbers = re.findall(r'\d+\.?\d*', price_clean)
        if numbers:
            try:
                return float(numbers[0])
            except ValueError:
                return 0.0
        
        return 0.0
    
    def _extract_specs_fixed(self, product: Dict) -> Dict:
        """Extrait les spécifications"""
        specs = {}
        
        # Champs directs
        direct_fields = ['storage', 'ram', 'battery_health', 'color']
        for field in direct_fields:
            value = product.get(field)
            if value and str(value).upper() not in ['NULL', 'NONE', '']:
                specs[field] = str(value).strip()
        
        # Condition
        condition = product.get('condition')
        if condition and str(condition).upper() not in ['NULL', 'NONE', '']:
            specs['condition'] = str(condition).strip()
        
        return specs
    
    def _create_product_id_fixed(self, brand: str, model: str, product: Dict) -> str:
        """Crée un ID produit UNIQUE et STABLE"""
        # Nettoyer la marque
        clean_brand = re.sub(r'[^a-z0-9]', '', brand.lower())
        if not clean_brand or clean_brand == "unknown":
            clean_brand = "unknown"
        
        # Nettoyer le modèle
        if model == "Unknown":
            # Fallback: utiliser les premiers mots du titre
            title = product.get('title', '')
            words = re.findall(r'\b[a-z]+\d+\w*\b', title.lower())
            clean_model = words[0] if words else "unknown"
        else:
            clean_model = re.sub(r'[^a-z0-9]', '', model.lower())
        
        # Si toujours unknown, utiliser un hash du titre
        if clean_model == "unknown":
            import hashlib
            title = product.get('title', '')
            title_hash = hashlib.md5(title.encode()).hexdigest()[:8]
            clean_model = f"title_{title_hash}"
        
        # ID final
        product_id = f"{clean_brand}_{clean_model}"
        
        # DEBUG
        logger.debug(f"ID généré: {product_id} pour {brand} {model}")
        
        return product_id
    
    def _determine_condition_fixed(self, product: Dict) -> str:
        """Détermine la condition du produit"""
        condition = product.get('condition')
        
        if not condition or str(condition).upper() in ['NULL', 'NONE', '']:
            return 'used'  # Par défaut pour Avito
        
        cond_str = str(condition).lower()
        
        condition_map = {
            'neuf': 'new', 'new': 'new', 'nouveau': 'new',
            'bon': 'good', 'good': 'good', 'excellent': 'good',
            'moyen': 'fair', 'fair': 'fair', 'acceptable': 'fair',
            'mauvais': 'poor', 'poor': 'poor', 'endommagé': 'poor',
            'comme neuf': 'like new', 'like new': 'like new',
            'refurbished': 'refurbished', 'reconditionné': 'refurbished'
        }
        
        for key, value in condition_map.items():
            if key in cond_str:
                return value
        
        return 'used'
    
    def _build_url_fixed(self, product: Dict) -> str:
        """Construit l'URL correcte"""
        url = product.get('url')
        if url and 'avito.ma' in url:
            return url
        
        # Fallback: construire depuis ad_id
        ad_id = product.get('ad_id')
        if ad_id:
            return f"https://www.avito.ma/vi/{ad_id}.htm"
        
        return "https://www.avito.ma/"