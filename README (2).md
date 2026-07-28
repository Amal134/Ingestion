# Pipeline Data Engineering KBO & CBSO

Pipeline de données consacré aux entreprises belges, construit à partir :

- des fichiers Open Data de la **Banque-Carrefour des Entreprises**, KBO/BCE ;
- des comptes annuels publics disponibles auprès de la **Centrale des bilans de la Banque Nationale de Belgique**, NBB/CBSO.

Le projet met en place :

1. une ingestion des fichiers CSV KBO dans MongoDB ;
2. une couche **Bronze** regroupant toutes les informations d’une entreprise dans un document MongoDB ;
3. une couche **Silver** nettoyée, traduite et restructurée ;
4. un scraper responsable des comptes annuels CBSO ;
5. un stockage des fichiers téléchargés dans HDFS ;
6. une collection MongoDB de suivi des opérations de scraping ;
7. un environnement Docker regroupant MongoDB, HDFS, JupyterLab et trois proxies Tor.

---

## Sommaire

- [Architecture](#architecture)
- [Structure du projet](#structure-du-projet)
- [Sources de données](#sources-de-données)
- [Environnement technique](#environnement-technique)
- [Installation](#installation)
- [Configuration](#configuration)
- [Ordre d’exécution](#ordre-dexécution)
- [Couche Bronze](#couche-bronze)
- [Couche Silver](#couche-silver)
- [Scraping des comptes annuels](#scraping-des-comptes-annuels)
- [Stockage HDFS](#stockage-hdfs)
- [Suivi du scraping](#suivi-du-scraping)
- [Gestion des erreurs et limitation de débit](#gestion-des-erreurs-et-limitation-de-débit)
- [Schéma de la collection Silver](#schéma-de-la-collection-silver)
- [Interfaces et ports](#interfaces-et-ports)
- [Limites et évolutions possibles](#limites-et-évolutions-possibles)

---

## Architecture

```text
                         KBO Open Data
                    9 fichiers CSV relationnels
                               │
                               ▼
                    Import CSV vers MongoDB
                               │
                               ▼
                  Collections techniques kbo_*
                               │
                               ▼
                  Pipeline d’agrégation MongoDB
                               │
                               ▼
                     Collection entreprise
                        Couche Bronze
                               │
                               ▼
                 Nettoyage et transformation Python
                               │
                               ▼
                Collection entreprise_silver
                         Couche Silver


               Numéros d’entreprise sélectionnés
                               │
                               ▼
                    API publique NBB / CBSO
                               │
                               ▼
                  Téléchargement des dépôts CSV
                               │
                 ┌─────────────┴─────────────┐
                 ▼                           ▼
         Stockage des fichiers HDFS   Suivi dans MongoDB
```

---

## Structure du projet

```text
.
├── docker-compose.yml
├── hadoop.env
├── Scraping_Amel.ipynb
├── TD_construction_bronze (1).ipynb
├── TD_silver_Amel.ipynb
├── meta.csv
├── code.csv
├── enterprise.csv
├── establishment.csv
├── branch.csv
├── denomination.csv
├── address.csv
├── contact.csv
├── activity.csv
└── README.md
```

### Rôle des fichiers principaux

| Fichier | Description |
|---|---|
| `docker-compose.yml` | Déploie MongoDB, HDFS, JupyterLab et trois instances Tor |
| `TD_construction_bronze (1).ipynb` | Importe les CSV KBO et construit la collection Bronze `entreprise` |
| `TD_silver_Amel.ipynb` | Nettoie et transforme `entreprise` vers `entreprise_silver` |
| `Scraping_Amel.ipynb` | Récupère les comptes annuels publics CBSO et les stocke dans HDFS |
| `hadoop.env` | Configuration des services Hadoop |
| `*.csv` | Export Open Data KBO utilisé par les notebooks |

> Le fichier `hadoop.env` est référencé dans `docker-compose.yml`. Il doit être présent avant le démarrage des services HDFS.

---

## Sources de données

### KBO Open Data

La KBO, ou Banque-Carrefour des Entreprises, publie ses données sous forme de fichiers CSV indépendants.

Le projet utilise les neuf fichiers suivants :

| Fichier | Collection MongoDB | Description |
|---|---|---|
| `meta.csv` | `kbo_meta` | Métadonnées de l’export |
| `code.csv` | `kbo_code` | Référentiel des codes et traductions |
| `enterprise.csv` | `kbo_enterprise` | Entreprises |
| `establishment.csv` | `kbo_establishment` | Unités d’établissement |
| `branch.csv` | `kbo_branch` | Succursales |
| `denomination.csv` | `kbo_denomination` | Dénominations |
| `address.csv` | `kbo_address` | Adresses |
| `contact.csv` | `kbo_contact` | Coordonnées de contact |
| `activity.csv` | `kbo_activity` | Activités et codes NACE |

Les données sont organisées autour de trois types d’entités :

| Niveau | Clé | Description |
|---|---|---|
| Entreprise | `EnterpriseNumber` | Personne juridique |
| Établissement | `EstablishmentNumber` | Unité opérationnelle belge |
| Succursale | `Id` | Implantation belge d’une entreprise étrangère |

Les fichiers de détail utilisent la colonne commune `EntityNumber`. Cette colonne peut correspondre au numéro d’une entreprise, d’un établissement ou d’une succursale.

### NBB / CBSO

Les comptes annuels sont récupérés depuis la Centrale des bilans de la Banque Nationale de Belgique.

Endpoints utilisés :

```text
GET https://consult.cbso.nbb.be/api/rs-consult/published-deposits
```

Liste les dépôts associés à une entreprise.

```text
GET https://consult.cbso.nbb.be/api/external/broker/public/deposits/consult/csv/{deposit_id}
```

Télécharge le fichier CSV correspondant à un dépôt.

---

## Environnement technique

Le projet utilise les technologies suivantes :

- Python ;
- JupyterLab ;
- MongoDB 8 ;
- PyMongo ;
- MongoDB Aggregation Framework ;
- HDFS / Hadoop 3.4 ;
- Docker et Docker Compose ;
- Requests ;
- Requests SOCKS ;
- Stem ;
- Tor ;
- CSV et JSON.

---

## Installation

### Prérequis

Installer :

- Docker ;
- Docker Compose ;
- Git, si le projet est récupéré depuis un dépôt distant.

Vérifier l’installation :

```bash
docker --version
docker compose version
```

### Préparer les données KBO

Placer les neuf fichiers CSV dans le dossier parent ou dans le dossier accessible par `KBO_DATA_DIR`.

### Démarrer l’environnement

```bash
docker compose up -d
```

Vérifier les conteneurs :

```bash
docker compose ps
```

Consulter les journaux :

```bash
docker compose logs -f
```

Arrêter les services :

```bash
docker compose down
```

Pour supprimer également les volumes persistants :

```bash
docker compose down -v
```

---

## Configuration

| Variable | Valeur Docker par défaut | Description |
|---|---|---|
| `KBO_DATA_DIR` | `/data/kbo` | Dossier contenant les fichiers KBO |
| `MONGO_URI` | `mongodb://mongo:27017` | URI MongoDB utilisée depuis Jupyter |
| `MONGO_DB` | `kbo` | Nom de la base MongoDB |
| `HDFS_URL` | `http://namenode:9870` | Endpoint WebHDFS |
| `HDFS_USER` | `root` | Utilisateur HDFS |
| `TOR_PASSWORD` | `kbotp2026` | Mot de passe du ControlPort Tor |
| `CBSO_MIN_INTERVAL` | `2.0` | Délai minimum entre deux requêtes CBSO |

Depuis la machine hôte, MongoDB est accessible avec :

```text
mongodb://localhost:27018
```

---

## Ordre d’exécution

Après le démarrage de Docker, ouvrir :

```text
http://localhost:8888
```

Exécuter les notebooks dans l’ordre suivant :

1. `TD_construction_bronze (1).ipynb`
2. `TD_silver_Amel.ipynb`
3. `Scraping_Amel.ipynb`

---

## Couche Bronze

Le notebook Bronze :

- importe les neuf fichiers CSV ;
- crée les collections techniques `kbo_*` ;
- crée les index nécessaires ;
- enrichit les entreprises avec leurs détails ;
- enrichit les établissements et les succursales ;
- produit la collection `entreprise`.

Les fichiers sont lus en streaming avec `csv.DictReader` et insérés dans MongoDB par lots.

La collection finale Bronze contient notamment :

```text
denominations
addresses
contacts
activities
establishments
branches
```

---

## Couche Silver

Le notebook Silver :

- lit la collection `entreprise` ;
- traduit les codes avec `kbo_code` ;
- nettoie les adresses et contacts ;
- déduplique les activités NACE ;
- restructure les tableaux en dictionnaires ;
- produit la collection `entreprise_silver`.

Les champs comme le statut, la forme juridique, la situation juridique et le type d’entreprise sont traduits en français.

Les activités sont réparties entre :

```text
main
secondary
```

Les établissements sont indexés par `EstablishmentNumber` et les succursales par `Id`.

---

## Scraping des comptes annuels

Le notebook de scraping :

- sélectionne les entreprises depuis MongoDB ;
- interroge l’API CBSO ;
- récupère la liste des dépôts ;
- télécharge les fichiers CSV disponibles ;
- stocke les fichiers dans HDFS ;
- suit l’état du traitement dans MongoDB ;
- gère les erreurs HTTP ;
- applique une limitation de débit ;
- peut utiliser trois instances Tor.

Les principaux statuts gérés sont :

```text
429
500
502
503
504
```

Le scraper respecte l’en-tête `Retry-After` lorsqu’il est présent et applique un backoff exponentiel sinon.

---

## Stockage HDFS

Les fichiers téléchargés sont stockés dans HDFS via WebHDFS.

Configuration Docker :

```text
HDFS_URL=http://namenode:9870
HDFS_USER=root
```

Avant chaque téléchargement, le notebook vérifie si le fichier existe déjà afin d’éviter les doublons.

---

## Suivi du scraping

Le suivi est assuré dans la collection MongoDB :

```text
scrape_tracking
```

Elle permet de :

- préparer une liste de travail ;
- récupérer le prochain lot ;
- enregistrer le statut ;
- reprendre un traitement interrompu ;
- éviter les traitements en double.

---

## Utilisation de Tor

Trois instances Tor sont définies :

| Instance | Port SOCKS | ControlPort |
|---|---:|---:|
| `tor1` | `9050` | `9051` |
| `tor2` | `9060` | `9061` |
| `tor3` | `9070` | `9071` |

Les connexions passent par `socks5h`.

---

## Interfaces et ports

| Service | Adresse |
|---|---|
| JupyterLab | `http://localhost:8888` |
| Interface HDFS | `http://localhost:9870` |
| DataNode | `http://localhost:9864` |
| MongoDB | `mongodb://localhost:27018` |
| Tor 1 | `localhost:9050` |
| Tor 2 | `localhost:9060` |
| Tor 3 | `localhost:9070` |

---

## Volumes Docker

| Volume | Utilisation |
|---|---|
| `kbo_mongo_data` | Données MongoDB |
| `kbo_hdfs_name` | Métadonnées du NameNode |
| `kbo_hdfs_data` | Données du DataNode |

MongoDB utilise la compression WiredTiger `zstd` avec un cache configuré à `6 Go`.

---

## Commandes utiles

### Ouvrir MongoDB

```bash
docker exec -it kbo-mongo mongosh
```

### Vérifier HDFS

```bash
docker exec -it namenode hdfs dfs -ls /
```

### Redémarrer JupyterLab

```bash
docker compose restart jupyter
```

---

## Limites et évolutions possibles

Évolutions possibles :

- création d’une couche Gold ;
- ajout d’un orchestrateur comme Airflow ou Prefect ;
- transformation des notebooks en scripts Python ;
- ajout de tests automatisés ;
- ajout d’un fichier `.env` ;
- ajout d’un tableau de bord BI ;
- meilleure supervision des téléchargements ;
- parallélisation contrôlée des traitements.

---

## Avertissement

Les données sont publiques, mais leur collecte doit respecter :

- les conditions d’utilisation des sources ;
- les limites de débit ;
- les directives du fournisseur ;
- la législation applicable ;
- les principes de collecte responsable.

---

## Auteur

**Amel Chafter**

Projet Data Engineering — Python, MongoDB, HDFS, Docker et Open Data KBO/CBSO.
