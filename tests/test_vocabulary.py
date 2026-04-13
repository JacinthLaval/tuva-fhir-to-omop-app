"""
Vocabulary / Terminology table validation.

Usage:
    python tests/test_vocabulary.py
"""

import os
import sys

import snowflake.connector

DB = "TRE_HEALTHCARE_DB"
SCHEMA = "FHIR_STAGING"

TERMINOLOGY_TABLES = {
    "LOINC_TO_OMOP": "loinc_code",
    "SNOMED_TO_OMOP": "snomed_code",
    "RXNORM_TO_OMOP": "rxnorm_code",
    "ICD10CM_TO_OMOP": "icd10cm_code",
    "DEMOGRAPHIC_TO_OMOP": "source_code",
    "CONCEPT": "concept_id",
    "CONCEPT_RELATIONSHIP": "concept_id_1",
}

KNOWN_MAPPINGS = [
    ("LOINC_TO_OMOP", "loinc_code", "8480-6", "omop_concept_id", 3004249),
    ("SNOMED_TO_OMOP", "snomed_code", "73211009", "omop_concept_id", 201826),
]


def connect():
    return snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "HealthcareDemos"
    )


def pf(ok, msg, detail=""):
    tag = "PASS" if ok else "FAIL"
    print(f"  [{tag}] {msg}" + (f"  ({detail})" if detail else ""))
    return ok


def main():
    conn = connect()
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE SI_DEMO_WH")
    passed = 0
    failed = 0

    print("=" * 50)
    print("Vocabulary Table Validation")
    print("=" * 50)

    print("\n--- Row Counts ---")
    for table, pk_col in TERMINOLOGY_TABLES.items():
        fq = f"{DB}.{SCHEMA}.{table}"
        try:
            rows = cur.execute(f"SELECT COUNT(*) FROM {fq}").fetchall()
            n = rows[0][0]
            ok = pf(n > 0, f"{table} has rows", f"{n} rows")
        except Exception as e:
            ok = pf(False, f"{table} exists", str(e)[:80])
        passed += ok
        failed += (not ok)

    print("\n--- Duplicate Primary Keys ---")
    for table, pk_col in TERMINOLOGY_TABLES.items():
        fq = f"{DB}.{SCHEMA}.{table}"
        try:
            if table == "CONCEPT_RELATIONSHIP":
                dup_sql = f"""
                    SELECT COUNT(*) FROM (
                        SELECT concept_id_1, concept_id_2, relationship_id
                        FROM {fq}
                        GROUP BY concept_id_1, concept_id_2, relationship_id
                        HAVING COUNT(*) > 1
                    )
                """
            elif table == "DEMOGRAPHIC_TO_OMOP":
                dup_sql = f"""
                    SELECT COUNT(*) FROM (
                        SELECT source_system, source_code, category
                        FROM {fq}
                        GROUP BY source_system, source_code, category
                        HAVING COUNT(*) > 1
                    )
                """
            else:
                dup_sql = f"""
                    SELECT COUNT(*) FROM (
                        SELECT {pk_col} FROM {fq}
                        GROUP BY {pk_col} HAVING COUNT(*) > 1
                    )
                """
            rows = cur.execute(dup_sql).fetchall()
            dups = rows[0][0]
            ok = pf(dups == 0, f"{table} no duplicate PKs", f"{dups} duplicates" if dups else "")
        except Exception as e:
            ok = pf(False, f"{table} PK check", str(e)[:80])
        passed += ok
        failed += (not ok)

    print("\n--- Known Mappings ---")
    for table, src_col, src_val, tgt_col, expected_id in KNOWN_MAPPINGS:
        fq = f"{DB}.{SCHEMA}.{table}"
        try:
            rows = cur.execute(
                f"SELECT {tgt_col} FROM {fq} WHERE {src_col} = '{src_val}'"
            ).fetchall()
            if rows:
                actual = rows[0][0]
                ok = pf(int(actual) == expected_id,
                        f"{table}: {src_col}={src_val} -> {tgt_col}",
                        f"expected {expected_id}, got {actual}")
            else:
                ok = pf(False, f"{table}: {src_col}={src_val} -> {tgt_col}", "row not found")
        except Exception as e:
            ok = pf(False, f"{table}: {src_col}={src_val}", str(e)[:80])
        passed += ok
        failed += (not ok)

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed")
    print(f"{'='*50}")

    cur.close()
    conn.close()
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
