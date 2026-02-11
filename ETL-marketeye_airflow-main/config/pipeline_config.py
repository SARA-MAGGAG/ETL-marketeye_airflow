# config/pipeline_config.py
from pathlib import Path
import os
from typing import Dict, List

class PipelineConfig:
    """Configuration centralisée pour le pipeline MarketEye"""
    
    def __init__(self):
        # Dossiers de données - CHEMINS ABSOLUS POUR DOCKER
        self.BASE_DIR = Path("/opt/airflow")
        
        # Dossiers sources
        self.RAW_DATA_DIR = self.BASE_DIR / "data" / "raw"
        self.PROCESSED_DATA_DIR = self.BASE_DIR / "data" / "processed"
        self.REPORTS_DIR = self.BASE_DIR / "data" / "reports"
        
        # Création des dossiers
        self._create_directories()
        
        # Mapping des marques pour normalisation
        self.brand_mapping = {
            'samsung': 'Samsung', 'samsng': 'Samsung', 'samsuung': 'Samsung',
            'apple': 'Apple', 'iphone': 'Apple',
            'huawei': 'Huawei', 'hauwei': 'Huawei',
            'xiaomi': 'Xiaomi', 'redmi': 'Xiaomi', 'poco': 'Xiaomi',
            'oppo': 'Oppo', 'realme': 'Realme',
            'nokia': 'Nokia', 'tecno': 'Tecno',
            'infinix': 'Infinix', 'vivo': 'Vivo',
            'honor': 'Honor', 'oneplus': 'OnePlus',
            'motorola': 'Motorola', 'moto': 'Motorola',
            'google': 'Google', 'pixel': 'Google',
            'sony': 'Sony', 'lg': 'LG'
        }
        
        self.log_directories()
    
    def _create_directories(self):
        """Crée tous les répertoires nécessaires"""
        directories = [self.RAW_DATA_DIR, self.PROCESSED_DATA_DIR, self.REPORTS_DIR]
        
        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                print(f"✅ Dossier créé: {directory}")
            except Exception as e:
                print(f"❌ Erreur création dossier {directory}: {e}")
    
    def log_directories(self):
        """Log les chemins des dossiers"""
        print("=" * 50)
        print("📁 CONFIGURATION DES DOSSIERS")
        print("=" * 50)
        print(f"BASE_DIR: {self.BASE_DIR}")
        print(f"RAW_DATA_DIR: {self.RAW_DATA_DIR} - Existe: {self.RAW_DATA_DIR.exists()}")
        print(f"PROCESSED_DATA_DIR: {self.PROCESSED_DATA_DIR} - Existe: {self.PROCESSED_DATA_DIR.exists()}")
        print(f"REPORTS_DIR: {self.REPORTS_DIR} - Existe: {self.REPORTS_DIR.exists()}")
        
        # Lister les fichiers dans raw
        if self.RAW_DATA_DIR.exists():
            raw_files = list(self.RAW_DATA_DIR.glob("*"))
            print(f"\n📂 Fichiers dans {self.RAW_DATA_DIR}:")
            for file in raw_files:
                print(f"  - {file.name}")
    
    def get_source_patterns(self) -> Dict[str, List[str]]:
        """Patterns pour détection des fichiers sources"""
        return {
            'jumia': ['jumia', 'android', 'product'],
            'electroplanet': ['electroplanet', 'electro'],
            'avito': ['avito', 'ads']
        }