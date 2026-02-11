# plugins/marketeye_operators.py
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from typing import Dict, List, Any, Optional
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# ============================================
# OPÉRATEURS D'EXTRACTION
# ============================================

class DataExtractionOperator(BaseOperator):
    """Opérateur pour l'extraction des données par source"""
    
    @apply_defaults
    def __init__(self, source: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source = source
        
    def execute(self, context):
        self.log.info(f"📥 Extraction des données {self.source.upper()}")
        
        try:
            # Importer dynamiquement pour éviter les problèmes de circular imports
            from config.pipeline_config import PipelineConfig
            
            # Configuration
            config = PipelineConfig()
            
            # Détection des fichiers
            source_files = self._detect_source_files(config, self.source)
            if not source_files:
                self.log.warning(f"Aucun fichier trouvé pour {self.source}")
                return 0
            
            # Charger l'extracteur approprié
            extractor = self._get_extractor(config, self.source)
            if not extractor:
                raise AirflowException(f"Extracteur non trouvé pour {self.source}")
            
            all_data = []
            
            for file_path in source_files:
                self.log.info(f"Traitement de {file_path.name}")
                data = extractor.extract(file_path)
                all_data.extend(data)
            
            # Transformation des données
            transformed_data = []
            for item in all_data:
                try:
                    transformed = extractor.transform(item)
                    if transformed:
                        transformed_data.append(transformed)
                except Exception as e:
                    self.log.warning(f"Erreur transformation produit {self.source}: {e}")
                    continue
            
            # Sauvegarde temporaire
            output_path = config.PROCESSED_DATA_DIR / f"{self.source}_transformed.json"
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(transformed_data, f, ensure_ascii=False, indent=2)
            
            self.log.info(f"✅ {self.source.upper()}: {len(transformed_data)} produits transformés")
            
            # Passage des données aux tâches suivantes
            context['task_instance'].xcom_push(
                key=f'{self.source}_data_path',
                value=str(output_path)
            )
            
            return len(transformed_data)
            
        except Exception as e:
            self.log.error(f"❌ Erreur extraction {self.source}: {e}")
            raise AirflowException(f"Extraction {self.source} échouée: {e}")
    
    def _detect_source_files(self, config, source: str) -> List[Path]:
        """Détecte les fichiers pour une source donnée"""
        patterns = config.get_source_patterns().get(source, [])
        files = []
        
        if not config.RAW_DATA_DIR.exists():
            self.log.warning(f"📁 Dossier {config.RAW_DATA_DIR} n'existe pas")
            return files
        
        for file_path in config.RAW_DATA_DIR.glob("*"):
            if file_path.is_file():
                filename_lower = file_path.name.lower()
                if any(pattern in filename_lower for pattern in patterns):
                    files.append(file_path)
        
        self.log.info(f"📁 {len(files)} fichier(s) trouvé(s) pour {source}")
        return files
    
    def _get_extractor(self, config, source: str):
        """Retourne l'extracteur approprié"""
        try:
            if source == 'jumia':
                from scripts.data_processors.jumia_extractor import JumiaExtractor
                return JumiaExtractor(config)
            elif source == 'avito':
                from scripts.data_processors.avito_extractor import AvitoExtractor
                return AvitoExtractor(config)
            elif source == 'electroplanet':
                from scripts.data_processors.electroplanet_extractor import ElectroplanetExtractor
                return ElectroplanetExtractor(config)
        except ImportError as e:
            self.log.error(f"❌ Impossible d'importer l'extracteur {source}: {e}")
            return None

# ============================================
# OPÉRATEUR DE FUSION
# ============================================

class DataMergingOperator(BaseOperator):
    """Opérateur pour la fusion et déduplication des données"""
    
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    def execute(self, context):
        self.log.info("🔄 Fusion et déduplication des données")
        
        try:
            from config.pipeline_config import PipelineConfig
            config = PipelineConfig()
            
            # Récupération des données de toutes les sources
            all_products = []
            sources = ['jumia', 'avito', 'electroplanet']
            
            for source in sources:
                data_path = context['task_instance'].xcom_pull(
                    task_ids=f'extract_{source}_data',
                    key=f'{source}_data_path'
                )
                
                if data_path and Path(data_path).exists():
                    with open(data_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        all_products.extend(data)
                    self.log.info(f"📁 {source}: {len(data)} produits chargés")
                else:
                    self.log.warning(f"⚠️ Fichier {source} non trouvé ou vide")
            
            if not all_products:
                self.log.warning("⚠️ Aucune donnée à fusionner")
                all_products = []  # Liste vide plutôt qu'erreur
            
            # Fusion des produits
            merged_products = self._merge_products(all_products)
            
            # Déduplication
            final_products = self._remove_duplicates(merged_products)
            
            # Sauvegarde finale
            output_path = config.PROCESSED_DATA_DIR / "marketeye_final.json"
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(final_products, f, ensure_ascii=False, indent=2)
            
            self.log.info(f"✅ Fusion terminée: {len(final_products)} produits uniques")
            
            context['task_instance'].xcom_push(
                key='final_data_path',
                value=str(output_path)
            )
            
            return len(final_products)
            
        except Exception as e:
            self.log.error(f"❌ Erreur fusion: {e}")
            raise AirflowException(f"Fusion échouée: {e}")
    
    def _merge_products(self, products: List[Dict]) -> List[Dict]:
        """Fusionne les produits identiques"""
        merged_dict = {}
        
        for product in products:
            product_id = product.get('product_id')
            if not product_id:
                continue  # Ignorer les produits sans ID
            
            if product_id in merged_dict:
                existing = merged_dict[product_id]
                existing['offers'].extend(product.get('offers', []))
                
                # Fusionner les spécifications
                for key, value in product.get('specifications', {}).items():
                    if key not in existing['specifications'] or not existing['specifications'][key]:
                        existing['specifications'][key] = value
                
                # Mettre à jour les métadonnées
                existing['metadata']['sources'] = list(set(
                    existing['metadata'].get('sources', []) + 
                    product['metadata'].get('sources', [])
                ))
                existing['metadata']['last_updated'] = datetime.now().isoformat()
                
                # Garder le meilleur nom de produit
                if len(product.get('product_name', '')) > len(existing.get('product_name', '')):
                    existing['product_name'] = product['product_name']
                    
            else:
                merged_dict[product_id] = product
                
        return list(merged_dict.values())
    
    def _remove_duplicates(self, products: List[Dict]) -> List[Dict]:
        """Supprime les doublons intra-sources"""
        seen_offers = set()
        deduplicated = []
        
        for product in products:
            unique_offers = []
            
            for offer in product.get('offers', []):
                offer_id = f"{offer.get('source', '')}{offer.get('price', 0)}{offer.get('url', '')}"
                
                if offer_id not in seen_offers:
                    seen_offers.add(offer_id)
                    unique_offers.append(offer)
            
            if unique_offers:
                product['offers'] = unique_offers
                deduplicated.append(product)
                
        return deduplicated

# ============================================
# OPÉRATEUR DE STATISTIQUES
# ============================================

class StatisticsOperator(BaseOperator):
    """Opérateur pour le calcul des statistiques"""
    
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    def execute(self, context):
        self.log.info("📊 Calcul des statistiques")
        
        try:
            from config.pipeline_config import PipelineConfig
            config = PipelineConfig()
            
            data_path = context['task_instance'].xcom_pull(
                task_ids='merge_data',
                key='final_data_path'
            )
            
            if not data_path or not Path(data_path).exists():
                self.log.warning("⚠️ Fichier de données final non trouvé, génération de stats vides")
                products = []
            else:
                with open(data_path, 'r', encoding='utf-8') as f:
                    products = json.load(f)
            
            stats = self._calculate_statistics(products)
            
            # Sauvegarde des statistiques
            stats_path = config.PROCESSED_DATA_DIR / "dataset_statistics.json"
            with open(stats_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=2)
            
            # Génération CSV
            csv_success = self._generate_csv(products, config)
            
            self.log.info(f"✅ Statistiques calculées: {stats['total_products']} produits")
            
            context['task_instance'].xcom_push(
                key='statistics',
                value=stats
            )
            
            return stats
            
        except Exception as e:
            self.log.error(f"❌ Erreur calcul statistiques: {e}")
            raise AirflowException(f"Calcul statistiques échoué: {e}")
    
    def _calculate_statistics(self, products: List[Dict]) -> Dict:
        """Calcule les statistiques du dataset"""
        all_prices = []
        brand_count = {}
        source_count = {}
        condition_count = {}
        
        for product in products:
            brand = product.get('brand', 'Unknown')
            brand_count[brand] = brand_count.get(brand, 0) + 1
            
            for offer in product.get('offers', []):
                source = offer.get('source', 'Unknown')
                source_count[source] = source_count.get(source, 0) + 1
                
                condition = offer.get('condition', 'Inconnu')
                condition_count[condition] = condition_count.get(condition, 0) + 1
                
                price = offer.get('price', 0)
                if price and price > 0:
                    all_prices.append(price)
        
        stats = {
            "total_products": len(products),
            "total_offers": sum(len(p.get('offers', [])) for p in products),
            "sources_count": source_count,
            "brand_distribution": dict(sorted(brand_count.items(), key=lambda x: x[1], reverse=True)),
            "condition_distribution": condition_count,
            "price_stats": {
                'min': min(all_prices) if all_prices else 0,
                'max': max(all_prices) if all_prices else 0,
                'avg': sum(all_prices) / len(all_prices) if all_prices else 0,
                'total_offers': len(all_prices)
            },
            "generated_at": datetime.now().isoformat()
        }
        
        return stats
    
    def _generate_csv(self, products: List[Dict], config) -> bool:
        """Génère un fichier CSV pour analyse"""
        try:
            csv_data = []
            
            for product in products:
                for offer in product.get('offers', []):
                    row = {
                        'product_id': product.get('product_id', ''),
                        'brand': product.get('brand', ''),
                        'model': product.get('model', ''),
                        'product_name': product.get('product_name', ''),
                        'source': offer.get('source', ''),
                        'price': offer.get('price', 0),
                        'currency': offer.get('currency', 'MAD'),
                        'condition': offer.get('condition', 'N/A'),
                        'rating': offer.get('rating', 'N/A'),
                        'url': offer.get('url', 'N/A'),
                        'seller_type': offer.get('seller_type', 'N/A'),
                        'storage': product.get('specifications', {}).get('storage', 'N/A'),
                        'ram': product.get('specifications', {}).get('ram', 'N/A')
                    }
                    csv_data.append(row)
            
            if csv_data:
                df = pd.DataFrame(csv_data)
                csv_file = config.PROCESSED_DATA_DIR / "marketeye_clean.csv"
                df.to_csv(csv_file, index=False, encoding='utf-8')
                self.log.info(f"📄 Fichier CSV généré: {csv_file}")
                return True
            else:
                self.log.warning("⚠️ Aucune donnée à exporter en CSV")
                return False
                
        except Exception as e:
            self.log.error(f"❌ Erreur génération CSV: {e}")
            return False

# ============================================
# OPÉRATEUR DE RAPPORT
# ============================================

class ReportOperator(BaseOperator):
    """Opérateur pour la génération de rapports"""
    
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    def execute(self, context):
        self.log.info("📄 Génération du rapport")
        
        try:
            from config.pipeline_config import PipelineConfig
            config = PipelineConfig()
            
            stats = context['task_instance'].xcom_pull(
                task_ids='calculate_statistics',
                key='statistics'
            )
            
            if not stats:
                self.log.warning("⚠️ Statistiques non disponibles, génération rapport vide")
                stats = {
                    'total_products': 0,
                    'total_offers': 0,
                    'sources_count': {},
                    'price_stats': {'min': 0, 'max': 0, 'avg': 0, 'total_offers': 0},
                    'brand_distribution': {},
                    'condition_distribution': {}
                }
            
            report = self._generate_report(stats)
            
            # Sauvegarde du rapport
            report_path = config.REPORTS_DIR / f"etl_report_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report)
            
            self.log.info(f"✅ Rapport généré: {report_path}")
            
            return str(report_path)
            
        except Exception as e:
            self.log.error(f"❌ Erreur génération rapport: {e}")
            raise AirflowException(f"Génération rapport échouée: {e}")
    
    def _generate_report(self, stats: Dict) -> str:
        """Génère un rapport textuel"""
        # Formatage sécurisé des données
        total_products = stats.get('total_products', 0)
        total_offers = stats.get('total_offers', 0)
        
        sources = stats.get('sources_count', {})
        source_names = ', '.join(sources.keys()) if sources else 'Aucune'
        
        price_stats = stats.get('price_stats', {})
        min_price = price_stats.get('min', 0)
        max_price = price_stats.get('max', 0)
        avg_price = price_stats.get('avg', 0)
        total_price_offers = price_stats.get('total_offers', 0)
        
        report = f"""
RAPPORT ETL MARKETEYE - {datetime.now().strftime('%Y-%m-%d %H:%M')}
{'=' * 60}

📊 RÉSUMÉ DES DONNÉES:
├─ Produits uniques: {total_products}
├─ Offres totales: {total_offers}
├─ Sources: {source_names}

💰 STATISTIQUES PRIX (MAD):
├─ Prix minimum: {min_price:.2f}
├─ Prix maximum: {max_price:.2f}
├─ Prix moyen: {avg_price:.2f}
└─ Total offres valides: {total_price_offers}

🏷️ TOP 5 MARQUES:"""
        
        brand_dist = stats.get('brand_distribution', {})
        for i, (brand, count) in enumerate(list(brand_dist.items())[:5], 1):
            report += f"\n    {i}. {brand}: {count} produits"
        
        if not brand_dist:
            report += "\n    Aucune marque disponible"
        
        report += f"\n\n🌐 DISTRIBUTION PAR SOURCE:"
        for source, count in sources.items():
            report += f"\n    ├─ {source.capitalize()}: {count} offres"
        
        if not sources:
            report += "\n    Aucune source disponible"
        
        report += f"\n\n📦 CONDITIONS:"
        cond_dist = stats.get('condition_distribution', {})
        for condition, count in cond_dist.items():
            report += f"\n    ├─ {condition.capitalize()}: {count} produits"
        
        if not cond_dist:
            report += "\n    Aucune information de condition disponible"
        
        report += f"\n\n✅ Pipeline ETL exécuté avec succès!"
        report += f"\n{'=' * 60}"
        
        return report