"""Generate small, deterministic, entirely made-up music datasets (dummy data only).

    python seed/generate_seed_data.py

Writes:
  seed/supabase_seed.sql         -> genres / artists / songs; run in the Supabase SQL editor
  seed/mongo_playlists.json      -> nested playlist documents; load with seed/load_mongo.py
  seed/s3_song_streams.parquet   -> daily stream counts; upload to your S3 bucket
"""

from __future__ import annotations

import json
import random
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
rng = random.Random(7)

GENRES = ["Pop", "Rock", "Hip-Hop", "Electronic", "Jazz", "Classical", "Folk", "R&B"]
COUNTRIES = ["USA", "UK", "Turkey", "Germany", "Brazil", "Japan", "Sweden", "Canada"]
ARTIST_NAMES = [
    "Neon Harbor",
    "The Paper Tigers",
    "Lumen Drift",
    "Velvet Static",
    "Aria Quinn",
    "Mono Garden",
    "Echo Parade",
    "Saffron Hill",
    "Quiet Orbit",
    "Delta Moon",
    "Violet Arcade",
    "Ruby Lanes",
    "Northern Tide",
    "Glass Canyon",
    "Marlow & Co",
    "Pixel Sparrow",
    "Cinder Road",
    "Halcyon Days",
    "Open Window",
    "Atlas Bloom",
]
WORDS_A = [
    "Midnight",
    "Golden",
    "Broken",
    "Electric",
    "Silent",
    "Paper",
    "Velvet",
    "Wild",
    "Faded",
    "Hollow",
]
WORDS_B = [
    "Skyline",
    "Summer",
    "Signal",
    "Rivers",
    "Dreams",
    "Static",
    "Garden",
    "Motion",
    "Letters",
    "Echoes",
]


def build_catalog() -> tuple[list[tuple], list[tuple], list[tuple]]:
    genres = [(i, name) for i, name in enumerate(GENRES, start=1)]
    artists = [
        (i, name, rng.choice(COUNTRIES), rng.randint(1985, 2022))
        for i, name in enumerate(ARTIST_NAMES, start=1)
    ]
    songs = []
    for i in range(1, 101):
        songs.append(
            (
                i,
                f"{rng.choice(WORDS_A)} {rng.choice(WORDS_B)}",
                rng.randint(1, len(artists)),
                rng.randint(1, len(genres)),
                rng.randint(120, 380),
                rng.randint(2005, 2026),
                rng.random() < 0.15,
            )
        )
    return genres, artists, songs


def q(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def write_supabase_sql(genres, artists, songs) -> None:
    lines = [
        "-- Dummy music catalog for the Supabase source. Safe to re-run.",
        "drop table if exists songs; drop table if exists artists; drop table if exists genres;",
        "create table genres (id int primary key, name text not null);",
        "create table artists (id int primary key, name text not null, country text, formed_year int);",
        "create table songs (",
        "  id int primary key,",
        "  title text not null,",
        "  artist_id int not null references artists(id),",
        "  genre_id int not null references genres(id),",
        "  duration_seconds int,",
        "  release_year int,",
        "  explicit boolean);",
    ]
    lines += [f"insert into genres values ({i}, {q(n)});" for i, n in genres]
    lines += [f"insert into artists values ({i}, {q(n)}, {q(c)}, {y});" for i, n, c, y in artists]
    lines += [
        f"insert into songs values ({i}, {q(t)}, {a}, {g}, {d}, {y}, {str(e).lower()});"
        for i, t, a, g, d, y, e in songs
    ]
    (ROOT / "seed" / "supabase_seed.sql").write_text("\n".join(lines) + "\n")


def write_playlists(songs) -> None:
    docs = []
    for i in range(1, 31):
        docs.append(
            {
                "_id": f"pl_{i:03d}",
                "name": f"{rng.choice(WORDS_A)} {rng.choice(['Mix', 'Sessions', 'Radio', 'Picks'])} {i}",
                "owner": {
                    "displayName": f"user_{rng.randint(100, 999)}",
                    "country": rng.choice(COUNTRIES),
                },
                "tags": rng.sample(
                    ["chill", "workout", "focus", "party", "road-trip", "study"],
                    k=rng.randint(1, 3),
                ),
                "songIds": sorted(rng.sample([s[0] for s in songs], k=rng.randint(5, 15))),
                "stats": {
                    "followers": rng.randint(0, 50000),
                    "totalPlays": rng.randint(100, 2_000_000),
                },
                "isPublic": rng.random() < 0.7,
                "createdYear": rng.randint(2018, 2026),
            }
        )
    (ROOT / "seed" / "mongo_playlists.json").write_text(json.dumps(docs, indent=2) + "\n")


def write_streams(songs) -> None:
    rows = []
    n = 0
    for day in range(1, 11):
        for country in COUNTRIES[:5]:
            for song in rng.sample([s[0] for s in songs], k=4):
                n += 1
                rows.append(
                    {
                        "stream_id": f"st_{n:05d}",
                        "chart_date": f"2026-10-{day:02d}",  # text on purpose; the pipeline casts it to DATE
                        "country": country,
                        "song_id": song,
                        "streams": rng.randint(1_000, 900_000),
                    }
                )
    pd.DataFrame(rows).to_parquet(ROOT / "seed" / "s3_song_streams.parquet", index=False)


if __name__ == "__main__":
    genres, artists, songs = build_catalog()
    write_supabase_sql(genres, artists, songs)
    write_playlists(songs)
    write_streams(songs)
    print("Seed data written.")
