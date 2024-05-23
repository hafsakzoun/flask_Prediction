# Prédiction de désabonnement des clients d’opérateur de Télécommunication en temps réel

## Description du projet

Ce projet a pour but de développer une application Web permettant de prédire en temps réel le désabonnement des clients d'un opérateur de télécommunication. L'application utilise Apache Kafka Streams pour le traitement en temps réel des données, PySpark pour les prétraitements et les modèles de machine learning, et MongoDB pour la sauvegarde des résultats. L'interface Web est développée avec Flask.

## Structure du projet

- **docker-compose.yml**: Configuration Docker pour déployer les services nécessaires (Kafka, Zookeeper, MongoDB, Spark).
- **kafka/**: Contient les scripts pour le producteur et le consommateur Kafka.
  - **consumer.py**: Lit les messages de Kafka, prédit le désabonnement des clients et enregistre les résultats dans MongoDB.
  - **producer.py**: Lit les données du fichier CSV, les transforme et les envoie à Kafka.
- **model/**: Contient les scripts pour l'entraînement et la prédiction des modèles de machine learning.
  - **entrenment.py**: Entraîne les modèles de machine learning sur les données d'entraînement.
  - **prediction.py**: Utilise le modèle entraîné pour prédire les désabonnements sur les nouvelles données.

## Prérequis

- Docker
- Docker Compose
- Python 3.8+
- Apache Kafka
- PySpark
- MongoDB
- Flask

## Installation

1. **Cloner le repository GitHub**:
    ```bash
    git clone https://github.com/votre-utilisateur/votre-repository.git
    cd votre-repository
    ```

2. **Configurer et lancer les services Docker**:
    Assurez-vous que Docker et Docker Compose sont installés, puis exécutez :
    ```bash
    docker-compose up -d
    ```

3. **Installer les dépendances Python**:
    ```bash
    pip install -r requirements.txt
    ```

## Utilisation

1. **Démarrer le producteur Kafka**:
    ```bash
    python kafka/producer.py
    ```

2. **Démarrer le consommateur Kafka**:
    ```bash
    python kafka/consumer.py
    ```

3. **Entraîner le modèle**:
    ```bash
    python model/entrenment.py
    ```

4. **Prédire avec le modèle**:
    ```bash
    python model/prediction.py
    ```

## Configuration Docker

Le fichier `docker-compose.yml` configure les services suivants :
- **Zookeeper**: Service nécessaire pour Kafka.
- **Kafka Broker**: Service Kafka pour la gestion des topics et des partitions.
- **MongoDB**: Base de données pour stocker les résultats de prédiction.
- **Spark**: Service Spark pour exécuter les tâches de traitement de données et de machine learning.

## Schéma des données

Les fichiers de données utilisés (`churn-bigml-80.csv` pour l'entraînement et `churn-bigml-20.csv` pour les tests) contiennent les colonnes suivantes :

- `state`: string
- `account_length`: integer
- `area_code`: integer
- `international_plan`: string
- `voice_mail_plan`: string
- `number_vmail_messages`: integer
- `total_day_minutes`: double
- `total_day_calls`: integer
- `total_day_charge`: double
- `total_eve_minutes`: double
- `total_eve_calls`: integer
- `total_eve_charge`: double
- `total_night_minutes`: double
- `total_night_calls`: integer
- `total_night_charge`: double
- `total_intl_minutes`: double
- `total_intl_calls`: integer
- `total_intl_charge`: double
- `customer_service_calls`: integer
- `churn`: string (cible à prédire)

## Architecture de l'application

1. **Lecture des données en temps réel avec Kafka Streams**: Les données sont lues à partir de fichiers CSV et envoyées à un topic Kafka.
2. **Prétraitement des données avec PySpark**: Les données sont nettoyées, transformées et mises en forme pour l'entraînement des modèles.
3. **Entraînement des modèles de machine learning**: Utilisation de PySpark MLlib pour entraîner plusieurs modèles supervisés.
4. **Sauvegarde du meilleur modèle**: Le modèle avec les meilleures performances est sauvegardé pour une utilisation ultérieure.
5. **Prédiction en temps réel**: Le modèle sauvegardé est utilisé pour prédire le désabonnement des clients en temps réel.
6. **Sauvegarde des résultats dans MongoDB**: Les résultats des prédictions sont sauvegardés dans une base de données MongoDB.
7. **Présentation des résultats via une application Web**: Une interface Web permet d'afficher les résultats des prédictions et d'interagir avec les données.

## Déploiement

Uploader le projet sur GitHub :
```bash
git add .
git commit -m "Initial commit"
git push origin main
