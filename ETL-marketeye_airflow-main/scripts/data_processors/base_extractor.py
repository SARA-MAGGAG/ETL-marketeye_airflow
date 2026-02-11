# scripts/data_processors/base_extractor.py
import json
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class BaseExtractor(ABC):
    """Classe de base pour tous les extracteurs"""
    
    def __init__(self, config):
        self.config = config
        self.master_schema = {
            "product_id": None,
            "brand": None,
            "model": None,
            "product_name": None,
            "category": "Smartphone",
            "specifications": {},
            "offers": [],
            "metadata": {
                "created_at": datetime.now().isoformat(),
                "sources": [],
                "last_updated": None
            }
        }
    
    def safe_string(self, value: Any) -> str:
        """Convertit n'importe quelle valeur en string de manière sécurisée"""
        if value is None:
            return ""
        if isinstance(value, (int, float)):
            return str(value)
        return str(value)
    
    def normalize_brand(self, brand: Optional[str]) -> str:
        """Normalise le nom des marques"""
        if not brand:
            return "Unknown"
        
        brand_str = self.safe_string(brand)
        brand_lower = brand_str.lower().strip()
        
        for key, value in self.config.brand_mapping.items():
            if key in brand_lower:
                return value
        return brand_str.title()
    
    def extract_model_from_title(self, title: Any, brand: Any) -> str:
        """Extrait le modèle depuis le titre du produit"""
        if not title:
            return "Unknown"
            
        title_str = self.safe_string(title)
        brand_str = self.safe_string(brand)
        title_clean = title_str.lower()
        brand_lower = brand_str.lower()
        title_clean = title_clean.replace(brand_lower, "").strip()
        
        samsung_patterns = [
            r'galaxy\s+([a-z]\d+\w*\s*\d*\w*)',
            r'([a-z]\d+\w*\s*\d*\w*)\s+',
        ]
        
        generic_patterns = [
            r'(\d+\s*go|\d+\s*gb)',
            r'(\d+\s*go\s+\d+\s*go\s+ram)',
            r'([a-z]+\s*\d+\w*)',
        ]
        
        if 'samsung' in brand_lower:
            patterns = samsung_patterns + generic_patterns
        else:
            patterns = generic_patterns
            
        for pattern in patterns:
            match = re.search(pattern, title_clean)
            if match:
                model = match.group(1).upper()
                model = re.sub(r'\s+', ' ', model).strip()
                return model
                
        return "Unknown"
    
    def clean_price(self, price_str: Any) -> float:
        """Nettoie et convertit les prix en float"""
        if not price_str:
            return 0.0
            
        if isinstance(price_str, (int, float)):
            return float(price_str)
            
        price_clean = re.sub(r'[^\d,.]', '', self.safe_string(price_str))
        price_clean = price_clean.replace(',', '.')
        
        numbers = re.findall(r'\d+\.?\d*', price_clean)
        return float(numbers[0]) if numbers else 0.0
    
    def create_product_id(self, brand: str, model: str, title: str) -> str:
        """Crée un ID produit unique"""
        clean_brand = re.sub(r'[^a-z0-9]', '', self.safe_string(brand).lower())
        clean_model = re.sub(r'[^a-z0-9]', '', self.safe_string(model).lower())
        
        if clean_model == "unknown":
            title_clean = re.sub(r'[^a-z0-9]', ' ', self.safe_string(title).lower())
            words = title_clean.split()
            if len(words) > 1:
                clean_model = words[1]
        
        return f"{clean_brand}_{clean_model}"
    
    @abstractmethod
    def extract(self, file_path: Path) -> List[Dict]:
        """Méthode abstraite pour l'extraction"""
        pass
    
    @abstractmethod
    def transform(self, raw_data: Dict) -> Dict:
        """Méthode abstraite pour la transformation"""
        pass
    # Dans base_extractor.py, ajoutez ces méthodes :
    
    def extract_price_from_string(self, price_str: str) -> float:
        """Extrait le prix depuis une chaîne comme '250 DH' ou '1,200.50 MAD'"""
        if not price_str:
            return 0.0
            
        # Supprimer tout sauf les chiffres, points et virgules
        price_clean = re.sub(r'[^\d,.]', '', str(price_str))
        price_clean = price_clean.replace(',', '')
        
        try:
            return float(price_clean)
        except ValueError:
            return 0.0
    
    def clean_model_name(self, model: str) -> str:
        """Nettoie le nom du modèle"""
        if not model:
            return "Unknown"
        
        # Supprimer les caractères spéciaux, garder lettres, chiffres, espaces
        cleaned = re.sub(r'[^a-zA-Z0-9\s]', ' ', str(model))
        # Supprimer les espaces multiples
        cleaned = re.sub(r'\s+', ' ', cleaned).strip().upper()
        
        return cleaned if cleaned else "Unknown"