import json
import math
from pathlib import Path

def clean_json_file(file_path: Path):
    """Nettoie un fichier JSON des valeurs NaN"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        def clean_value(value):
            if value is None:
                return None
            elif isinstance(value, float) and math.isnan(value):
                return None
            elif isinstance(value, str) and value.lower() in ['nan', 'none', 'null']:
                return None
            elif isinstance(value, dict):
                return {k: clean_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [clean_value(item) for item in value]
            else:
                return value
        
        cleaned_data = clean_value(data)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(cleaned_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Fichier nettoyé: {file_path}")
        return True
        
    except Exception as e:
        print(f"❌ Erreur nettoyage {file_path}: {e}")
        return False

# Nettoyer tous les fichiers Avito
raw_dir = Path("data/raw")
for file_path in raw_dir.glob("*avito*"):
    if file_path.is_file():
        clean_json_file(file_path)