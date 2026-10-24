"""Generate small, deterministic demo datasets (dummy data only — nothing real).

    python seed/generate_seed_data.py

Writes:
  data/city_cost_of_living.csv     -> served over HTTPS from the repo (raw.githubusercontent.com)
  seed/mongo_companies.json        -> load with seed/load_mongo.py
  seed/sheets_skills_demand.csv    -> import into a new Google Sheet (File > Import)
  seed/supabase_seed.sql           -> run in the Supabase SQL editor
"""

from __future__ import annotations

import csv
import json
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
rng = random.Random(42)

CITIES = [
    ("Istanbul", "Turkey"),
    ("Berlin", "Germany"),
    ("Lisbon", "Portugal"),
    ("Warsaw", "Poland"),
    ("Amsterdam", "Netherlands"),
    ("Madrid", "Spain"),
    ("Dublin", "Ireland"),
    ("Prague", "Czechia"),
    ("Vienna", "Austria"),
    ("Budapest", "Hungary"),
    ("Tallinn", "Estonia"),
    ("Athens", "Greece"),
    ("Toronto", "Canada"),
    ("Austin", "USA"),
    ("Denver", "USA"),
    ("Singapore", "Singapore"),
    ("Tokyo", "Japan"),
    ("Seoul", "South Korea"),
    ("Sydney", "Australia"),
    ("Dubai", "UAE"),
    ("Sao Paulo", "Brazil"),
    ("Mexico City", "Mexico"),
    ("Bangalore", "India"),
    ("Cape Town", "South Africa"),
]
SKILLS = [
    ("SQL", "data"),
    ("Python", "language"),
    ("Airflow", "orchestration"),
    ("dbt", "transformation"),
    ("Spark", "processing"),
    ("Databricks", "platform"),
    ("BigQuery", "warehouse"),
    ("Snowflake", "warehouse"),
    ("Kafka", "streaming"),
    ("Docker", "devops"),
    ("Kubernetes", "devops"),
    ("Terraform", "devops"),
    ("AWS", "cloud"),
    ("GCP", "cloud"),
    ("Azure", "cloud"),
    ("Git", "tooling"),
    ("Pandas", "library"),
    ("Pydantic", "library"),
    ("FastAPI", "framework"),
    ("Postgres", "database"),
    ("MongoDB", "database"),
    ("Looker", "bi"),
    ("Tableau", "bi"),
    ("Power BI", "bi"),
]
INDUSTRIES = ["fintech", "health", "retail", "logistics", "media", "energy", "gaming", "education"]
SENIORITY = ["junior", "mid", "senior", "lead"]
TITLES = ["Data Engineer", "Analytics Engineer", "Data Analyst", "ML Engineer", "Platform Engineer"]


def write_cities() -> None:
    path = ROOT / "data" / "city_cost_of_living.csv"
    path.parent.mkdir(exist_ok=True)
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(
            [
                "City",
                "Country",
                "Population (millions)",
                "Avg Rent USD",
                "Avg Salary USD",
                "Internet Mbps",
                "Last Updated",
            ]
        )
        for city, country in CITIES:
            w.writerow(
                [
                    city,
                    country,
                    round(rng.uniform(0.4, 15.0), 2),
                    rng.randrange(600, 3200, 50),
                    rng.randrange(18000, 110000, 500),
                    rng.randrange(40, 400, 5),
                    "2026-10-01",
                ]
            )


def write_companies() -> None:
    docs = []
    for i in range(1, 26):
        city, country = rng.choice(CITIES)
        docs.append(
            {
                "_id": f"co_{i:03d}",
                "name": f"{rng.choice(['Nimbus', 'Quanta', 'Verdant', 'Helix', 'Orbit', 'Lumen', 'Cobalt', 'Atlas'])} "
                f"{rng.choice(['Labs', 'Systems', 'Works', 'Group', 'Analytics'])} {i}",
                "foundedYear": rng.randint(1998, 2023),
                "industry": rng.choice(INDUSTRIES),
                "address": {"city": city, "country": country},
                "tags": rng.sample(
                    ["remote-first", "hiring", "series-a", "profitable", "b2b", "open-source"],
                    k=rng.randint(1, 3),
                ),
                "funding": {
                    "totalUsdMillions": round(rng.uniform(0, 250), 1),
                    "rounds": rng.randint(0, 5),
                },
                "employeeCount": rng.choice([12, 45, 120, 380, 900, 2400]),
            }
        )
    (ROOT / "seed" / "mongo_companies.json").write_text(json.dumps(docs, indent=2) + "\n")


def write_skills() -> None:
    with open(ROOT / "seed" / "sheets_skills_demand.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["skill", "category", "demand_score", "trend", "updated_on"])
        for skill, cat in SKILLS:
            w.writerow(
                [
                    skill,
                    cat,
                    round(rng.uniform(20, 98), 1),
                    rng.choice(["up", "flat", "down"]),
                    "2026-10-01",
                ]
            )


def write_supabase_sql() -> None:
    lines = [
        "-- Dummy data for the Supabase source. Safe to re-run.",
        "drop table if exists job_postings; drop table if exists employers;",
        "create table employers (id int primary key, name text not null, industry text, hq_city text);",
        "create table job_postings (",
        "  id int primary key, employer_id int not null references employers(id), title text not null,",
        "  seniority text, salary_min int, salary_max int, remote boolean, posted_at timestamptz);",
    ]
    for i in range(1, 13):
        city, _ = rng.choice(CITIES)
        lines.append(
            f"insert into employers values ({i}, 'Employer {i:02d}', '{rng.choice(INDUSTRIES)}', '{city}');"
        )
    for i in range(1, 61):
        lo = rng.randrange(35000, 120000, 1000)
        day = rng.randint(1, 28)
        lines.append(
            f"insert into job_postings values ({i}, {rng.randint(1, 12)}, '{rng.choice(TITLES)}', "
            f"'{rng.choice(SENIORITY)}', {lo}, {lo + rng.randrange(5000, 40000, 1000)}, "
            f"{rng.choice(['true', 'false'])}, '2026-09-{day:02d}T09:00:00Z');"
        )
    (ROOT / "seed" / "supabase_seed.sql").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    write_cities()
    write_companies()
    write_skills()
    write_supabase_sql()
    print("Seed data written.")
