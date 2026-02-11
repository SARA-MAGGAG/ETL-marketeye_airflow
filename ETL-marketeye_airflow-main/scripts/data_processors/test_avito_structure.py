# scripts/data_processors/test_avito_structure.py
import sys
import os
from pathlib import Path

# Ajouter le chemin parent pour les imports
current_dir = Path(__file__).parent.parent.parent  # Remonter à marketeye_airflow
sys.path.insert(0, str(current_dir))

import json
from scripts.data_processors.avito_extractor import AvitoExtractor
from config.pipeline_config import PipelineConfig

def test_avito_extractor():
    """Teste l'extracteur Avito avec la nouvelle structure"""
    
    # Données de test
    test_data = {
        "ad_id": "76741338",
        "title": "Samsung S24 ULTRA - 512 GB",
        "description": "Téléphone neuf scellé",
        "price": "7800 DH",
        "city": "Casablanca",
        "area": "Maarif",
        "seller_type": "STORE",
        "seller_name": "Phone Store",
        "category": "Smartphone et Téléphone",
        "url": "https://www.avito.ma/vi/57312179.htm",
        "list_time": "2025-12-14T12:52:03Z",
        "brand": "SAMSUNG",
        "model": "S24 ULTRA",
        "storage": "512GB",
        "ram": "12GB",
        "battery_health": "100%",
        "color": "Noir",
        "condition": "NEUF",
        "model_clean": True,
        "model_word_count": 2
    }
    
    config = PipelineConfig()
    extractor = AvitoExtractor(config)
    
    # Tester la transformation
    result = extractor.transform(test_data)
    
    print("📊 RÉSULTAT DU TEST AVITO - SAMSUNG S24 ULTRA")
    print("=" * 60)
    
    if result is None:
        print("❌ La transformation a échoué!")
        return
    
    print(f"✅ Brand: {result['brand']} (attendu: Samsung)")
    print(f"✅ Model: {result['model']} (attendu: S24 ULTRA)")
    print(f"✅ Price: {result['offers'][0]['price']} (attendu: 7800.0)")
    print(f"✅ Condition: {result['offers'][0]['condition']} (attendu: new)")
    print(f"✅ Storage: {result['specifications'].get('storage', 'N/A')} (attendu: 512GB)")
    print(f"✅ Product ID: {result['product_id']}")
    print(f"✅ URL: {result['offers'][0]['url']}")
    
    # Vérifications
    assert result['brand'] == 'Samsung', f"❌ Brand incorrect: {result['brand']}"
    assert result['model'] == 'S24 ULTRA', f"❌ Model incorrect: {result['model']}"
    assert result['offers'][0]['price'] == 7800.0, f"❌ Price incorrect: {result['offers'][0]['price']}"
    assert result['offers'][0]['condition'] == 'new', f"❌ Condition incorrect: {result['offers'][0]['condition']}"
    
    print("\n" + "=" * 60)
    print("🎉 Tous les tests passent avec succès !")
    print("\nDonnées transformées complètes:")
    print(json.dumps(result, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    test_avito_extractor()