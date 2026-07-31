r"""Controle de sante du pipeline KBO.

Usage :  .\.venv\Scripts\python.exe verifier_pipeline.py

Verifie, dans l'ordre ou les pannes se propagent : la memoire de la VM Docker
(cause racine des blocages precedents), les conteneurs, MongoDB, HDFS, les
proxies Tor et l'API de la Centrale des bilans. Chaque ligne porte son verdict ;
un bilan final indique si le projet est operationnel.
"""

import json
import subprocess
import sys

ATTENDU_CONTENEURS = {
    "kbo-mongo", "namenode", "datanode",
    "tor1", "tor2", "tor3", "kbo-jupyter", "kbo-airflow",
}

# Collections que le pipeline doit avoir produites, avec leur ordre de grandeur
# minimal : un compte nettement inferieur signale un import incomplet.
ATTENDU_COLLECTIONS = {
    "kbo_enterprise": 1_000_000,
    "entreprise": 1_000_000,
    "entreprise_silver": 1_000_000,
    "kbo_code": 1_000,
}

problemes: list[str] = []
avertissements: list[str] = []


def verdict(ok: bool, libelle: str, detail: str, *, bloquant: bool = True) -> None:
    """Affiche une ligne de controle et enregistre l'anomalie eventuelle."""
    marque = "OK  " if ok else ("ECHEC" if bloquant else "ATTENTION")
    print(f"  [{marque:<9}] {libelle:<34} {detail}")
    if not ok:
        (problemes if bloquant else avertissements).append(libelle)


def section(titre: str) -> None:
    print(f"\n{titre}")
    print("  " + "-" * 74)


# --------------------------------------------------------------- 1. la VM
section("1. Memoire de la VM Docker  (cause racine des blocages precedents)")
try:
    sortie = subprocess.run(
        ["wsl", "-d", "docker-desktop", "--", "free", "-m"],
        capture_output=True, text=True, timeout=60,
    ).stdout
    mem = next(l.split() for l in sortie.splitlines() if l.startswith("Mem:"))
    swap = next(l.split() for l in sortie.splitlines() if l.startswith("Swap:"))
    total, dispo = int(mem[1]), int(mem[6])
    swap_utilise = int(swap[2])

    verdict(total >= 10_000, "memoire allouee a la VM",
            f"{total} Mo  (>= 10000 attendu, via .wslconfig)")
    verdict(dispo >= 1_000, "memoire encore disponible", f"{dispo} Mo")
    # Le swap qui se remplit est le signe avant-coureur du blocage total.
    verdict(swap_utilise < 200, "swap consomme",
            f"{swap_utilise} Mo  (doit rester proche de 0)", bloquant=False)
except Exception as exc:
    verdict(False, "lecture de la memoire", f"{type(exc).__name__}: {exc}")


# --------------------------------------------------------- 2. conteneurs
section("2. Conteneurs Docker")
try:
    sortie = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}|{{.Status}}"],
        capture_output=True, text=True, timeout=60,
    )
    if sortie.returncode != 0:
        verdict(False, "API Docker", sortie.stderr.strip()[:60])
    else:
        actifs = dict(
            l.split("|", 1) for l in sortie.stdout.splitlines() if "|" in l
        )
        for nom in sorted(ATTENDU_CONTENEURS):
            etat = actifs.get(nom)
            verdict(etat is not None, nom, etat or "absent")
except Exception as exc:
    verdict(False, "docker ps", f"{type(exc).__name__}: {exc}")


# ----------------------------------------------------------- 3. MongoDB
section("3. MongoDB  (localhost:27018)")
try:
    import pymongo

    db = pymongo.MongoClient(
        "mongodb://localhost:27018", serverSelectionTimeoutMS=8000
    )["kbo"]
    db.command("ping")
    verdict(True, "connexion", "le serveur repond")

    for nom, minimum in ATTENDU_COLLECTIONS.items():
        n = db[nom].estimated_document_count()
        verdict(n >= minimum, f"collection {nom}", f"{n:,} documents")

    # Sans cet index, l'echantillonnage du scraping sature mongod.
    index = set(db.entreprise.index_information())
    verdict("JuridicalForm_1" in index, "index sur JuridicalForm",
            ", ".join(sorted(index)))

    meta = {d.get("Variable"): d.get("Value") for d in db.kbo_meta.find()}
    verdict(bool(meta.get("ExtractNumber")), "extrait charge",
            f"n{chr(176)} {meta.get('ExtractNumber')} "
            f"({meta.get('ExtractType')}, {meta.get('SnapshotDate')})")

    suivi = db.scrape_tracking.count_documents({})
    faits = db.scrape_tracking.count_documents({"status": "done"})
    verdict(suivi > 0, "file de scraping",
            f"{faits}/{suivi} entreprises traitees", bloquant=False)
except Exception as exc:
    verdict(False, "MongoDB", f"{type(exc).__name__}: {str(exc)[:60]}")


# -------------------------------------------------------------- 4. HDFS
section("4. HDFS  (localhost:9870)")
try:
    import requests

    r = requests.get(
        "http://localhost:9870/webhdfs/v1/?op=LISTSTATUS", timeout=10
    )
    verdict(r.status_code == 200, "NameNode", f"HTTP {r.status_code}")

    r = requests.get(
        "http://localhost:9870/webhdfs/v1/data/raw?op=LISTSTATUS", timeout=10
    )
    if r.status_code == 200:
        dossiers = [e["pathSuffix"] for e in r.json()["FileStatuses"]["FileStatus"]]
        verdict(len(dossiers) > 0, "donnees scrapees",
                f"{len(dossiers)} entreprise(s) : {', '.join(dossiers[:5])}")
    else:
        verdict(False, "donnees scrapees", "/data/raw absent", bloquant=False)
except Exception as exc:
    verdict(False, "HDFS", f"{type(exc).__name__}: {str(exc)[:60]}")


# --------------------------------------------------------------- 5. Tor
section("5. Proxies Tor  (rotation d'IP pour le scraping)")
try:
    import requests

    ips = []
    for nom, port in (("tor1", 9050), ("tor2", 9060), ("tor3", 9070)):
        try:
            proxy = f"socks5h://localhost:{port}"
            ip = requests.get(
                "https://api.ipify.org",
                proxies={"http": proxy, "https": proxy},
                timeout=25,
            ).text.strip()
            ips.append(ip)
            verdict(True, f"{nom} (socks {port})", f"sortie {ip}")
        except Exception as exc:
            verdict(False, f"{nom} (socks {port})",
                    f"{type(exc).__name__}", bloquant=False)
    verdict(len(set(ips)) == len(ips) and len(ips) > 1, "IP toutes distinctes",
            f"{len(set(ips))} IP differentes sur {len(ips)}", bloquant=False)
except Exception as exc:
    verdict(False, "Tor", f"{type(exc).__name__}: {str(exc)[:60]}", bloquant=False)


# ---------------------------------------------------------- 6. API CBSO
section("6. API Centrale des bilans  (source du scraping)")
try:
    import requests

    r = requests.get(
        "https://consult.cbso.nbb.be/api/rs-consult/published-deposits",
        params={"enterpriseNumber": "0693810613", "page": 0, "size": 50,
                "sort": ["periodEndDate,desc", "depositDate,desc"]},
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                 "Accept": "application/json"},
        timeout=30,
    )
    n = r.json().get("totalElements") if r.status_code == 200 else None
    verdict(r.status_code == 200, "appel de reference",
            f"HTTP {r.status_code}, {n} depots pour 0693810613")
except Exception as exc:
    verdict(False, "API CBSO", f"{type(exc).__name__}: {str(exc)[:60]}")


# ------------------------------------------------------------- bilan
print("\n" + "=" * 78)
if problemes:
    print(f"  {len(problemes)} PROBLEME(S) BLOQUANT(S) : {', '.join(problemes)}")
elif avertissements:
    print(f"  PIPELINE OPERATIONNEL  ({len(avertissements)} point(s) "
          f"d'attention : {', '.join(avertissements)})")
else:
    print("  PIPELINE ENTIEREMENT OPERATIONNEL : tous les controles sont au vert.")
print("=" * 78)

sys.exit(1 if problemes else 0)
