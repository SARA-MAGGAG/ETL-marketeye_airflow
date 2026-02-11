# scripts/data_processors/jumia_extractor.py
from .base_extractor import BaseExtractor
import re
from typing import Dict, List, Any  # ⬅️ IMPORT MANQUANT !
from pathlib import Path
from datetime import datetime

class JumiaExtractor(BaseExtractor):
    """Extracteur spécialisé pour Jumia"""
    
    def extract(self, file_path: Path) -> List[Dict]:
        """Extrait les données Jumia depuis un fichier"""
        return self.load_json_file(file_path)
    
    def transform(self, raw_product: Dict) -> Dict:
        """Transforme un produit Jumia"""
        try:
            brand = self.normalize_brand(raw_product.get('brand'))
            model = self.extract_model_from_title(raw_product.get('title', ''), brand)
            
            product_id = self.create_product_id(brand, model, raw_product.get('title', ''))
            
            offer = {
                "source": "Jumia",
                "price": self.clean_price(raw_product.get('price')),
                "original_price": self.clean_price(raw_product.get('old_price')),
                "currency": "MAD",
                "condition": "Neuf",
                "rating": self.extract_rating(raw_product.get('rating')),
                "reviews_count": raw_product.get('reviews_count_text'),
                "url": raw_product.get('product_url'),
                "scraped_at": raw_product.get('scraped_at')
            }
            
            master_product = self.master_schema.copy()
            master_product.update({
                "product_id": product_id,
                "brand": brand,
                "model": model,
                "product_name": raw_product.get('title'),
                "specifications": self.extract_specs_jumia(raw_product),
                "offers": [offer],
                "metadata": {
                    "sources": ["Jumia"],
                    "created_at": datetime.now().isoformat(),
                    "last_updated": datetime.now().isoformat()
                }
            })
            
            return master_product
            
        except Exception as e:
            logger.warning(f"Erreur transformation produit Jumia: {e}")
            return None
    
    def extract_rating(self, rating_data: Any) -> float:
        """Extrait la note numérique"""
        if not rating_data:
            return 0.0
            
        if isinstance(rating_data, (int, float)):
            return float(rating_data)
            
        rating_str = self.safe_string(rating_data)
        
        out_of_match = re.search(r'(\d+\.?\d*)\s*out of\s*\d+', rating_str)
        if out_of_match:
            return float(out_of_match.group(1))
            
        slash_match = re.search(r'(\d+\.?\d*)\s*/\s*\d+', rating_str)
        if slash_match:
            return float(slash_match.group(1))
            
        decimal_match = re.search(r'(\d+\.?\d*)', rating_str)
        if decimal_match:
            return float(decimal_match.group(1))
            
        return 0.0
    
    def extract_specs_jumia(self, product: Dict) -> Dict:
        """Extrait les spécifications depuis Jumia"""
        specs = {}
        
        title = self.safe_string(product.get('title', ''))
        description = self.safe_string(product.get('description', ''))
        full_text = (title + " " + description).lower()
        
        storage_match = re.search(r'(\d+)\s*(go|gb|go ram)', full_text)
        if storage_match:
            specs['storage'] = f"{storage_match.group(1)} {storage_match.group(2).upper()}"
        
        ram_match = re.search(r'(\d+)\s*go\s*ram', full_text)
        if ram_match:
            specs['ram'] = f"{ram_match.group(1)} Go"
            
        screen_match = re.search(r'(\d+[.,]?\d*)"', full_text)
        if screen_match:
            specs['screen_size'] = f"{screen_match.group(1)}\""
            
        if product.get('specs'):
            for key, value in product['specs'].items():
                key_str = self.safe_string(key).lower()
                value_str = self.safe_string(value)
                if 'ram' in key_str and value_str:
                    specs['ram'] = value_str
                elif 'stockage' in key_str or 'storage' in key_str and value_str:
                    specs['storage'] = value_str
                elif 'écran' in key_str or 'screen' in key_str and value_str:
                    specs['screen_size'] = value_str
                    
        return specs