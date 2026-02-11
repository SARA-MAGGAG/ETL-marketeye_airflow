# ETL MarketEye - Airflow

Pipeline ETL automatisé pour le projet MarketEye utilisant Apache Airflow pour l'orchestration des workflows de données.

## Description

Ce projet implémente un pipeline ETL (Extract, Transform, Load) complet pour collecter, transformer et charger des données de marché. Il utilise Apache Airflow pour orchestrer et planifier les tâches de traitement de données.

## Technologies Utilisées

- **Apache Airflow** - Orchestration des workflows
- **Docker** - Containerisation de l'environnement
- **Python** - Langage principal pour les scripts ETL
- **SQL** - Gestion de base de données

## Structure du Projet

```
ETL-marketeye_airflow/
│
├── dags/                    # DAGs Airflow (workflows)
├── scripts/                 # Scripts Python pour l'ETL
├── config/                  # Fichiers de configuration
├── data/                    # Données d'entrée/sortie
├── plugins/                 # Plugins personnalisés Airflow
├── docker-compose.yml       # Configuration Docker
├── requirements.txt         # Dépendances Python
├── .env                     # Variables d'environnement
├── backup.sql              # Sauvegarde base de données
└── start_airflow.bat       # Script de démarrage Windows
```

## Prérequis

- Docker & Docker Compose
- Python 3.8+
- 4GB RAM minimum

## Installation

### 1. Cloner le repository

```bash
git clone https://github.com/rachidamaliki/ETL-marketeye_airflow.git
cd ETL-marketeye_airflow
```

### 2. Configuration de l'environnement

Créer/modifier le fichier `.env` avec vos configurations :

```env
AIRFLOW_UID=50000
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
```

### 3. Lancer avec Docker

```bash
# Démarrer les services
docker-compose up -d

# Vérifier que les services sont actifs
docker-compose ps
```

### Sous Windows

```bash
# Utiliser le script de démarrage
start_airflow.bat
```

## Utilisation

### Accéder à l'interface Airflow

1. Ouvrir le navigateur : `http://localhost:8080`
2. Identifiants par défaut :
   - Username: `airflow`
   - Password: `airflow`

### Lancer un DAG

1. Dans l'interface Airflow, activer le DAG souhaité
2. Cliquer sur "Trigger DAG" pour lancer manuellement
3. Surveiller l'exécution dans l'interface

## Pipeline ETL

Le pipeline comprend généralement les étapes suivantes :

1. **Extract** - Extraction des données depuis les sources
2. **Transform** - Nettoyage et transformation des données
3. **Load** - Chargement dans la base de données cible
4. **Validation** - Vérification de la qualité des données

## Restauration de la Base de Données

Pour restaurer la base de données depuis le backup :

```bash
# Avec Docker
docker exec -i postgres_container psql -U airflow -d marketeye < backup.sql

# Ou avec psql directement
psql -U airflow -d marketeye < marketeye_backup.sql
```

## Développement

### Ajouter un nouveau DAG

1. Créer un fichier Python dans le dossier `dags/`
2. Définir votre DAG avec les tâches nécessaires
3. Airflow détectera automatiquement le nouveau DAG

### Ajouter des dépendances

```bash
# Ajouter au requirements.txt
echo "nouvelle-dependance==version" >> requirements.txt

# Reconstruire les conteneurs
docker-compose down
docker-compose up -d --build
```

## Commandes Utiles

```bash
# Voir les logs
docker-compose logs -f

# Arrêter les services
docker-compose down

# Redémarrer un service spécifique
docker-compose restart airflow-webserver

# Nettoyer les volumes
docker-compose down -v
```

## Monitoring

- Interface Web Airflow : `http://localhost:8080`
- Flower (Celery monitoring) : `http://localhost:5555` (si configuré)

## Troubleshooting

### Problème de permissions
```bash
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### Réinitialiser Airflow
```bash
docker-compose down -v
docker-compose up -d
```

## Auteur

**Rachid Amaliki**
- GitHub: [@rachidamaliki](https://github.com/rachidamaliki)

## Licence

Ce projet est développé pour MarketEye.

---

**Note** : Assurez-vous de ne jamais committer le fichier `.env` avec des credentials sensibles.
