# Industrialisation d'un Pipeline ELT YouTube con Docker & Apache Airflow

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org)
[![Docker](https://img.shields.io/badge/Docker-Multi--Container-blue)](https://www.docker.com)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-teal)](https://airflow.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Data%20Warehouse-blue)](https://www.postgresql.org)

## 📌 Présentation du Projet
Ce projet consiste à industrialiser un pipeline de données complet en migrant des scripts de collecte locaux vers une architecture moderne, conteneurisée et orchestrée. 

L'objectif est d'extraire des données dynamiques depuis l'**API YouTube Data v3**, de les ingérer dans un environnement de staging, puis de les transformer pour alimenter la couche analytique (*Core*) d'un Data Warehouse **PostgreSQL**.

---

## 🏗️ Architecture Technique

Le projet est entièrement orchestré et isolé à l'aide de **Docker Compose**, qui pilote les services suivants :
* **Apache Airflow** (CeleryExecutor) : Planification et supervision du pipeline.
* **PostgreSQL** : Hébergement des métadonnées d'Airflow et du Data Warehouse (Divisé en schémas `staging` et `core`).
* **Redis** : Broker de messages pour la distribution des tâches Airflow.

### Flux de Données (ELT) :
```text
[API YouTube] ➔ [Airflow Task: Staging] ➔ [PostgreSQL: staging.yt_api]
                                                    │
                                          [Airflow Task: Core (Transformation)]
                                                    │
                                                    ▼
                                        [PostgreSQL: core.yt_api]
