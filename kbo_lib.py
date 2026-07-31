from __future__ import annotations

import re

# ============================================================ pipeline bronze

DETAIL_SOURCES = (
    ("kbo_denomination", "denominations"),
    ("kbo_address", "addresses"),
    ("kbo_contact", "contacts"),
    ("kbo_activity", "activities"),
)


def detail_lookups(primary_key: str) -> list[dict]:
    """Les 4 etapes rattachant a une entite ses informations complementaires."""
    return [
        {"$lookup": {"from": source,
                     "localField": primary_key,
                     "foreignField": "EntityNumber",
                     "as": alias}}
        for source, alias in DETAIL_SOURCES
    ]


def children_lookup(*, source: str, child_key: str, alias: str) -> dict:
    """Rattache a une entreprise ses entites filles, deja enrichies.

    `EnterpriseNumber` relie l'entreprise a sa fille ; `child_key` est la cle
    propre de la fille, utilisee a l'interieur du sous-pipeline.
    """
    return {"$lookup": {
        "from": source,
        "let": {"enterprise_number": "$EnterpriseNumber"},
        "pipeline": [
            {"$match": {"$expr": {"$eq": ["$EnterpriseNumber", "$$enterprise_number"]}}},
            *detail_lookups(child_key),
        ],
        "as": alias,
    }}


def bronze_stages() -> list[dict]:
    """Les etapes de jointure produisant un document `entreprise`."""
    return [
        *detail_lookups("EnterpriseNumber"),
        children_lookup(source="kbo_establishment",
                        child_key="EstablishmentNumber", alias="establishments"),
        children_lookup(source="kbo_branch", child_key="Id", alias="branches"),
    ]


# ============================================================ transformation silver

DEFAULT_COUNTRY = "Belgique"
CONTACT_KEYS = {"EMAIL": "email", "TEL": "phone", "WEB": "web", "FAX": "fax"}

# (nom de sortie, champ bronze, categorie kbo_code) - ordre alphabetique
SCALAR_FIELDS = (
    ("juridicalForm", "JuridicalForm", "JuridicalForm"),
    ("juridicalFormCAC", "JuridicalFormCAC", "JuridicalForm"),
    ("juridicalSituation", "JuridicalSituation", "JuridicalSituation"),
    ("status", "Status", "Status"),
    ("typeOfEnterprise", "TypeOfEnterprise", "TypeOfEnterprise"),
)

ADDRESS_FIELDS = (
    ("zipcode", "Zipcode"),
    ("municipality", "MunicipalityFR"),
    ("street", "StreetFR"),
    ("houseNumber", "HouseNumber"),
    ("box", "Box"),
)

_PARENTHESES = re.compile(r"\([^)]*\)")
_MULTI_SPACE = re.compile(r"\s+")


class SilverTransformer:
    """Applique les regles de la couche silver a un document bronze.

    Le referentiel `kbo_code` est charge une fois en memoire (7 000 lignes en
    francais) : chaque traduction devient un acces O(1), au lieu d'un `$lookup`
    par code a resoudre.
    """

    def __init__(self, codes: dict[tuple[str, str], str], *,
                 omit_empty_address_fields: bool = True):
        self.codes = codes
        self.omit_empty_address_fields = omit_empty_address_fields

    @classmethod
    def from_db(cls, db, language: str = "FR", **kwargs) -> "SilverTransformer":
        codes = {
            (doc["Category"], doc["Code"]): doc["Description"].strip()
            for doc in db.kbo_code.find(
                {"Language": language},
                {"_id": 0, "Category": 1, "Code": 1, "Description": 1})
        }
        return cls(codes, **kwargs)

    def translate(self, category: str, code, default=None):
        if not code:
            return default
        return self.codes.get((category, code), default)

    # ------------------------------------------------------------ fragments
    def scalars(self, bronze: dict) -> dict:
        """Les 5 champs plats traduits ; un champ vide ou inconnu est omis."""
        return {
            name: label
            for name, field, category in SCALAR_FIELDS
            if (label := self.translate(category, bronze.get(field)))
        }

    def denominations(self, rows) -> dict:
        """Tableau -> dict {type traduit: {language, denomination}} ; dernier gagne."""
        result = {}
        for row in rows:
            label = self.translate("TypeOfDenomination", row.get("TypeOfDenomination"))
            if not label:
                continue
            result[label] = {
                "language": self.translate("Language", row.get("Language"), ""),
                "denomination": row.get("Denomination", ""),
            }
        return result

    def country(self, raw: str | None) -> str:
        """'France (Metropole)' -> 'France' ; vide -> 'Belgique'."""
        value = _MULTI_SPACE.sub(" ", _PARENTHESES.sub(" ", raw or "")).strip()
        return value or DEFAULT_COUNTRY

    def addresses(self, rows) -> dict:
        """Tableau -> dict {type traduit: {country, zipcode, ...}}."""
        result = {}
        for row in rows:
            label = self.translate("TypeOfAddress", row.get("TypeOfAddress"))
            if not label:
                continue
            address = {"country": self.country(row.get("CountryFR"))}
            for name, field in ADDRESS_FIELDS:
                value = (row.get(field) or "").strip()
                if value or not self.omit_empty_address_fields:
                    address[name] = value
            result[label] = address
        return result

    def contacts(self, rows) -> dict:
        """Tableau -> dict {email?, phone?, web?, fax?} ; `EntityContact` jamais lu."""
        result = {}
        for row in rows:
            value = (row.get("Value") or "").strip()
            if not value:
                continue
            kind = row.get("ContactType", "")
            result[CONTACT_KEYS.get(kind, kind.lower())] = value
        return result

    def activities(self, rows) -> dict:
        """Dedoublonne puis repartit en {main, secondary}.

        Une meme activite reelle est souvent codee sous plusieurs versions NACE.
        On dedoublonne sur (activityGroup, description) *a l'interieur de chaque
        classification* -- la meme paire peut legitimement exister en `main` et
        en `secondary` -- et la version NACE la plus recente gagne. Le
        `NaceCode` brut n'est jamais conserve.
        """
        buckets: dict[str, dict] = {"main": {}, "secondary": {}}
        for row in rows:
            version = row.get("NaceVersion", "")
            description = self.translate(f"Nace{version}", row.get("NaceCode"))
            if not description:
                continue
            group = self.translate("ActivityGroup", row.get("ActivityGroup"), "")
            bucket = buckets["main" if row.get("Classification") == "MAIN" else "secondary"]
            key = (group, description)
            previous = bucket.get(key)
            if previous is None or version > previous["naceVersion"]:
                bucket[key] = {"activityGroup": group,
                               "description": description,
                               "naceVersion": version}
        return {name: list(entries.values()) for name, entries in buckets.items()}

    # ------------------------------------------------------------ entites filles
    def establishment(self, row: dict) -> dict:
        """Memes regles que l'entreprise, sans `EnterpriseNumber` (redondant)."""
        document = {}
        if row.get("StartDate"):
            document["startDate"] = row["StartDate"]
        document["denominations"] = self.denominations(row.get("denominations", ()))
        document["addresses"] = self.addresses(row.get("addresses", ()))
        document["contacts"] = self.contacts(row.get("contacts", ()))
        document["activities"] = self.activities(row.get("activities", ()))
        return document

    def branch(self, row: dict) -> dict:
        """Une succursale n'a jamais de denomination ni d'activite propre."""
        document = {}
        if row.get("StartDate"):
            document["startDate"] = row["StartDate"]
        document["addresses"] = self.addresses(row.get("addresses", ()))
        document["contacts"] = self.contacts(row.get("contacts", ()))
        return document

    # ------------------------------------------------------------ document complet
    def to_silver(self, bronze: dict) -> dict:
        """Document bronze -> document silver (fonction pure)."""
        document = {
            "_id": bronze["_id"],
            "enterpriseNumber": bronze.get("EnterpriseNumber") or bronze["_id"],
        }
        if bronze.get("StartDate"):
            document["startDate"] = bronze["StartDate"]

        document["denominations"] = self.denominations(bronze.get("denominations", ()))
        document["addresses"] = self.addresses(bronze.get("addresses", ()))
        document["contacts"] = self.contacts(bronze.get("contacts", ()))
        document["activities"] = self.activities(bronze.get("activities", ()))
        document["establishments"] = {
            row["EstablishmentNumber"]: self.establishment(row)
            for row in bronze.get("establishments", ())
        }
        document["branches"] = {row["Id"]: self.branch(row)
                                for row in bronze.get("branches", ())}
        document.update(self.scalars(bronze))
        return document
