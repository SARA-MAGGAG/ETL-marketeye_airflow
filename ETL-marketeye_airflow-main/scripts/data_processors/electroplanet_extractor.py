# scripts/data_processors/electroplanet_extractor.py
from .base_extractor import BaseExtractor
from typing import Dict, List, Any  # ⬅️ IMPORT MANQUANT !
from pathlib import Path
from datetime import datetime

class ElectroplanetExtractor(BaseExtractor):
    """Extracteur spécialisé pour Electroplanet"""
    
    def extract(self, file_path: Path) -> List[Dict]:
        """Extrait les données Electroplanet depuis un fichier"""
        return self.load_json_file(file_path)
    
    def transform(self, raw_product: Dict) -> Dict:
        """Transforme un produit Electroplanet"""
        try:
            brand = self.normalize_brand(raw_product.get('brand'))
            model = raw_product.get('specifications', {}).get('Modèle') or "Unknown"
            if model == "Unknown":
                model = self.extract_model_from_title(raw_product.get('name', ''), brand)
            
            product_id = self.create_product_id(brand, model, raw_product.get('name', ''))
            
            offer = {
                "source": "Electroplanet",
                "price": self.clean_price(raw_product.get('price')),
                "original_price": self.clean_price(raw_product.get('old_price')),
                "currency": "MAD",
                "condition": "Neuf",
                "rating": raw_product.get('reviews_summary', {}).get('average_rating'),
                "reviews_count": raw_product.get('reviews_summary', {}).get('total_reviews'),
                "url": raw_product.get('product_url'),
                "scraped_at": raw_product.get('detailed_scraped_at') or raw_product.get('scraped_at')
            }
            
            master_product = self.master_schema.copy()
            master_product.update({
                "product_id": product_id,
                "brand": brand,
                "model": model,
                "product_name": raw_product.get('name'),
                "specifications": self.extract_specs_electroplanet(raw_product),
                "offers": [offer],
                "metadata": {
                    "sources": ["Electroplanet"],
                    "created_at": datetime.now().isoformat(),
                    "last_updated": datetime.now().isoformat()
                }
            })
            
            return master_product
            
        except Exception as e:
            logger.warning(f"Erreur transformation produit Electroplanet: {e}")
            return None
    
    def extract_specs_electroplanet(self, product: Dict) -> Dict:
        """Extrait les spécifications depuis Electroplanet"""
        specs = {}
        product_specs = product.get('specifications', {})
        
        spec_mapping = {
            'Capacité de stockage interne': 'storage',
            'Capacité de la RAM': 'ram',
            'Marque': 'brand',
            'Modèle': 'model',
            'Résolution de la caméra arrière (numerique)': 'camera',
            'Famille de processeur': 'processor',
            'Afficher le nom du marketing technologique': 'screen_tech',
            'Écran Gorilla Glass': 'gorilla_glass'
        }
        
        for key, value in product_specs.items():
            if key in spec_mapping and value:
                specs[spec_mapping[key]] = self.safe_string(value)
                
        return specs