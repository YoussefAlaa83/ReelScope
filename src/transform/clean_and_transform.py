import glob
import fnmatch
import os
import json
import logging
from io import BytesIO
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.utils.azure_storage import upload_bytes_to_adls, download_blob_to_local, list_blobs
from src.utils.config import AZURE_CONTAINER_SILVER, DATA_DIR, RAW_DATA_DIR

PROCESSED_DIR = DATA_DIR / "processed"
LOGGER = logging.getLogger(__name__)

def download_bronze_files():
    """Resolve each required latest Bronze input, preferring cached local files."""
    patterns = ("tmdb_detailed_movies_*.json", "omdb_movies_*.json", "bom_movies_*.json")
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    resolved = {}
    for pattern in patterns:
        local_matches = sorted(RAW_DATA_DIR.glob(pattern), key=lambda p: (p.stat().st_mtime, p.name))
        if local_matches:
            resolved[pattern] = local_matches[-1]
            continue
        matching = [b for b in list_blobs(container="bronze") if fnmatch.fnmatch(b.name.rsplit("/", 1)[-1], pattern)]
        matching.sort(key=lambda b: (getattr(b, "last_modified", None) or datetime.min.replace(tzinfo=timezone.utc), b.name))
        if matching:
            blob = matching[-1]
            local_path = RAW_DATA_DIR / blob.name.rsplit("/", 1)[-1]
            if download_blob_to_local(blob.name, local_path, container="bronze"):
                resolved[pattern] = local_path
                continue
        raise FileNotFoundError(f"No input matching {pattern} found in {RAW_DATA_DIR} or Azure Bronze")
    return resolved

def get_spark():
    """Start a local Spark session."""
    return (SparkSession.builder
            .appName("ReelScope_Silver_Transform")
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .getOrCreate())

def latest_file(pattern):
    """Newest file in data/raw that matches the pattern (None if missing)."""
    files = sorted(glob.glob(os.path.join(str(RAW_DATA_DIR), pattern)), key=os.path.getmtime)
    return files[-1] if files else None


def read_movies(spark, pattern):
    """Read a raw JSON file ({"movies": [...]}) into a DataFrame with one row per movie."""
    path = latest_file(pattern)
    if path is None:
        print("No file found for", pattern)
        return None
    raw = spark.read.option("multiLine", True).json(path)
    return raw.select(F.explode("movies").alias("m")).select("m.*")

def clean_tmdb(tmdb):
    """One row per movie: valid imdb_id, no duplicates, 0 -> null, real date type."""
    df = (tmdb
          .withColumn("imdb_id", F.trim(F.lower(F.col("imdb_id"))))
          .filter(F.col("imdb_id").rlike("^tt[0-9]+$")))

    # keep one row per imdb_id (the one with most votes)
    payload_tie = F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)]))
    w = Window.partitionBy("imdb_id").orderBy(F.col("vote_count").desc_nulls_last(), F.col("id").asc_nulls_last(), F.col("title").asc_nulls_last(), payload_tie.asc())
    df = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    for c in ["budget", "revenue", "runtime"]:
        parsed = F.expr(f"try_cast(cast(`{c}` as string) as bigint)")
        df = df.withColumn(c, F.when(parsed > 0, parsed))  # 0 / invalid values mean unknown -> null
    return df.withColumn("release_date", F.to_date("release_date", "yyyy-MM-dd"))


def build_genres(movies):
    """dim_genre + bridge_movie_genre from the nested genres list."""
    g = movies.select("imdb_id", F.explode("genres").alias("g"))
    bridge = g.select("imdb_id", F.col("g.id").alias("genre_id")).dropDuplicates()
    dim = (g.select(F.col("g.id").alias("genre_id"), F.col("g.name").alias("genre_name"))
            .dropDuplicates(["genre_id"]))
    return dim, bridge


def build_companies(movies):
    """dim_production_company + bridge_movie_company from the nested production_companies list."""
    c = movies.select("imdb_id", F.explode("production_companies").alias("c"))
    bridge = c.select("imdb_id", F.col("c.id").alias("company_id")).dropDuplicates()
    dim = (c.select(F.col("c.id").alias("company_id"), F.col("c.name").alias("company_name"),
                    F.col("c.origin_country").alias("origin_country"))
            .dropDuplicates(["company_id"]))
    return dim, bridge

def build_people(movies):
    """dim_person + bridge_movie_cast_crew: top-5 billed actors and directors."""
    cast = (movies.select("imdb_id", F.explode("credits.cast").alias("p"))
            .filter(F.col("p.order") < 5)
            .select("imdb_id",
                    F.col("p.id").alias("person_id"), F.col("p.name").alias("person_name"),
                    F.col("p.gender").alias("gender"),
                    F.col("p.known_for_department").alias("known_for_department"),
                    F.col("p.popularity").alias("popularity"),
                    F.lit("Actor").alias("role"),
                    F.col("p.character").alias("character_name"),
                    F.col("p.order").alias("billing_order")))

    crew = (movies.select("imdb_id", F.explode("credits.crew").alias("p"))
            .filter(F.col("p.job") == "Director")
            .select("imdb_id",
                    F.col("p.id").alias("person_id"), F.col("p.name").alias("person_name"),
                    F.col("p.gender").alias("gender"),
                    F.col("p.known_for_department").alias("known_for_department"),
                    F.col("p.popularity").alias("popularity"),
                    F.lit("Director").alias("role"),
                    F.lit(None).cast("string").alias("character_name"),
                    F.lit(None).cast("long").alias("billing_order")))

    people = cast.unionByName(crew)
    bridge = (people.select("imdb_id", "person_id", "role", "character_name", "billing_order")
              .dropDuplicates(["imdb_id", "person_id", "role"]))
    person_dim = people.select("person_id", "person_name", "gender", "known_for_department", "popularity")
    person_tie = F.to_json(F.struct(*[F.col(c) for c in sorted(person_dim.columns)]))
    person_window = Window.partitionBy("person_id").orderBy(
        F.col("popularity").desc_nulls_last(), F.col("person_name").asc_nulls_last(), person_tie.asc())
    dim = person_dim.withColumn("_rn", F.row_number().over(person_window)).filter(F.col("_rn") == 1).drop("_rn")
    return dim, bridge


def build_dim_date(movies):
    """One row per distinct release date."""
    d = movies.select("release_date").filter(F.col("release_date").isNotNull()).dropDuplicates()
    return d.select(
        F.date_format("release_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("release_date").alias("full_date"),
        F.year("release_date").alias("year"),
        F.quarter("release_date").alias("quarter"),
        F.month("release_date").alias("month"),
        F.date_format("release_date", "MMMM").alias("month_name"),
        F.concat((F.floor(F.year("release_date") / 10) * 10).cast("string"), F.lit("s")).alias("decade"))

def na(col):
    """Text column: trim it and turn '' / 'N/A' into null."""
    c = F.trim(F.col(col))
    return F.when((c != "") & (c != "N/A"), c)


def num(col):
    """Text like '$203,227,580' or '2,617,722' -> double (null if not a number)."""
    return F.expr(
        f"try_cast(regexp_replace(cast(`{col}` as string), '[^0-9.]', '') as double)"
    )

def clean_omdb(omdb):
    """OMDb: strings -> proper types, one row per imdb_id."""

    source = (omdb
        .withColumn("_rotten_raw", F.get(
            F.filter(
                F.col("Ratings"),
                lambda r: r["Source"] == "Rotten Tomatoes"
            ),
            0
        ).getField("Value"))
        .withColumn("_oscar_wins", F.expr("try_cast(regexp_extract(Awards, 'Won ([0-9]+) Oscar', 1) as int)"))
        .withColumn("_total_wins", F.expr("try_cast(regexp_extract(Awards, '([0-9]+) wins?', 1) as int)"))
        .withColumn("_nominations_count", F.expr("try_cast(regexp_extract(Awards, '([0-9]+) nominations?', 1) as int)"))
        .withColumn("_rotten_score", F.expr("try_cast(regexp_replace(_rotten_raw, '%', '') as int)")))

    df = source.select(
        F.trim(F.lower(F.col("imdbID"))).alias("imdb_id"),
        na("Rated").alias("omdb_rated"),
        na("Awards").alias("awards_summary"),
        na("Poster").alias("poster_url"),
        F.expr("try_cast(regexp_replace(cast(`Metascore` as string), '[^0-9.]', '') as int)").alias("metascore"),
        num("imdbRating").alias("imdb_rating"),
        F.expr("try_cast(regexp_replace(cast(`imdbVotes` as string), '[^0-9.]', '') as bigint)").alias("imdb_votes"),
        F.expr("try_cast(regexp_replace(cast(`BoxOffice` as string), '[^0-9.]', '') as bigint)").alias("omdb_boxoffice"),
        F.col("_rotten_score").alias("rotten_tomatoes_score"),
        F.col("_oscar_wins").alias("oscar_wins"),
        F.col("_total_wins").alias("total_wins"),
        F.col("_nominations_count").alias("nominations_count")
    )

    window = Window.partitionBy("imdb_id").orderBy(
        F.col("imdb_votes").desc_nulls_last(),
        F.col("imdb_rating").desc_nulls_last(),
        F.col("metascore").desc_nulls_last()
        , F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)])).asc()
    )

    return (
        df.filter(F.col("imdb_id").rlike("^tt[0-9]+$"))
          .withColumn("row_num", F.row_number().over(window))
          .filter(F.col("row_num") == 1)
          .drop("row_num")
    )


def clean_bom(bom):
    """Box Office Mojo: money columns, 0 -> null, one row per imdb_id."""
    def money(col):
        c = F.expr(f"try_cast(regexp_replace(cast(`{col}` as string), '[^0-9]', '') as bigint)")
        return F.when(c > 0, c)

    selected = (bom.select(
        F.trim(F.lower(F.col("imdb_id"))).alias("imdb_id"),
        money("domestic_revenue").alias("bom_domestic"),
        money("international_revenue").alias("bom_international"),
        money("worldwide_revenue").alias("bom_worldwide"),
        money("opening_weekend").alias("opening_weekend"),
        na("distributor").alias("distributor"),
        na("mpaa_rating").alias("bom_mpaa"))
        )
    tie = F.to_json(F.struct(*[F.col(c) for c in sorted(selected.columns)]))
    return (selected.withColumn("_rn", F.row_number().over(Window.partitionBy("imdb_id").orderBy(F.col("bom_worldwide").desc_nulls_last(), F.col("bom_domestic").desc_nulls_last(), F.col("distributor").asc_nulls_last(), tie.asc())))
            .filter(F.col("_rn") == 1).drop("_rn"))

def build_dim_distributor(df):
    """One row per distributor ('Unknown' when missing) with a surrogate key."""
    names = df.select(F.coalesce(F.col("distributor"), F.lit("Unknown")).alias("distributor_name")).distinct()
    names = names.withColumn("_norm", F.lower(F.trim(F.col("distributor_name"))))
    known = names.filter(F.col("_norm") != "unknown").withColumn("distributor_key", F.xxhash64("_norm"))
    unknown = names.sparkSession.createDataFrame([(-1, "Unknown", "unknown")], ["distributor_key", "distributor_name", "_norm"])
    return known.unionByName(unknown).select("distributor_key", "distributor_name")


def build_fact(movies, omdb, bom):
    """fact_movies: one row per movie, TMDb + OMDb + Mojo combined, with profit and ROI."""
    df = (movies.join(omdb, "imdb_id", "left").join(bom, "imdb_id", "left")
          .withColumn("distributor_name", F.coalesce(F.col("distributor"), F.lit("Unknown"))))
    dim_distributor = build_dim_distributor(df)
    df = df.join(dim_distributor, "distributor_name", "left")

    budget = F.col("budget")
    worldwide = F.coalesce(F.col("bom_worldwide"), F.col("revenue"))   # Mojo first, TMDb as fallback
    domestic = F.col("bom_domestic")
    international = F.coalesce(F.col("bom_international"),
                               F.when(worldwide.isNotNull() & domestic.isNotNull(), worldwide - domestic))

    budget_tier = F.when(budget < 10_000_000, "Low").when(budget < 100_000_000, "Mid").otherwise("Blockbuster")
    total_wins = F.coalesce(F.col("total_wins"), F.lit(0))
    oscar_wins = F.coalesce(F.col("oscar_wins"), F.lit(0))
    fact = df.select(
        "imdb_id",
        F.col("id").alias("tmdb_id"),
        F.date_format("release_date", "yyyyMMdd").cast("int").alias("release_date_key"),
        "distributor_key",
        "title",
        "original_language",
        F.coalesce(F.col("bom_mpaa"), F.col("omdb_rated")).alias("mpaa_rating"),
        "awards_summary",
        "poster_url",
        budget.alias("budget"),
        domestic.alias("domestic_revenue"),
        international.alias("international_revenue"),
        worldwide.alias("worldwide_revenue"),
        "opening_weekend",
        "oscar_wins", "total_wins", "nominations_count",
        (oscar_wins > 0).alias("has_oscar_win"),
        budget_tier.alias("budget_tier"),
        F.when(worldwide > 0, F.round(domestic / worldwide * 100, 2)).alias("domestic_share_pct"),
        F.when(worldwide > 0, F.round(international / worldwide * 100, 2)).alias("international_share_pct"),
        F.round((F.col("imdb_rating") + F.col("vote_average")) / 2, 2).alias("consensus_rating"),
        F.current_timestamp().alias("etl_loaded_at"),
        F.when((budget > 0) & (worldwide > 0), worldwide - budget).alias("profit"),
        F.when((budget > 0) & (worldwide > 0), F.round((worldwide - budget) / budget * 100, 2)).alias("roi_percentage"),
        F.col("runtime").alias("runtime_minutes"),
        F.col("vote_average").alias("tmdb_rating"),
        F.col("vote_count").alias("tmdb_votes"),
        "imdb_rating", "imdb_votes", "metascore", "rotten_tomatoes_score", "popularity")
    return fact, dim_distributor


def check_integrity(tables):
    """Validate primary keys, bridge composite keys, and referential integrity."""
    fact = tables["fact_movies"]
    checks = {
        "fact: duplicate imdb_id": fact.count() - fact.select("imdb_id").distinct().count(),
        "fact: null imdb_id": fact.filter(F.col("imdb_id").isNull()).count(),
        "fact -> dim_date": fact.filter(F.col("release_date_key").isNotNull())
            .join(tables["dim_date"], fact["release_date_key"] == tables["dim_date"]["date_key"], "left_anti").count(),
        "fact -> dim_distributor": fact.join(tables["dim_distributor"], "distributor_key", "left_anti").count(),
        "genre bridge -> fact": tables["bridge_movie_genre"].join(fact, "imdb_id", "left_anti").count(),
        "genre bridge -> dim_genre": tables["bridge_movie_genre"].join(tables["dim_genre"], "genre_id", "left_anti").count(),
        "company bridge -> fact": tables["bridge_movie_company"].join(fact, "imdb_id", "left_anti").count(),
        "company bridge -> dim_production_company": tables["bridge_movie_company"].join(tables["dim_production_company"], "company_id", "left_anti").count(),
        "person bridge -> fact": tables["bridge_movie_cast_crew"].join(fact, "imdb_id", "left_anti").count(),
        "person bridge -> dim_person": tables["bridge_movie_cast_crew"].join(tables["dim_person"], "person_id", "left_anti").count(),
    }
    primary_keys = {
        "dim_date": ["date_key"], "dim_genre": ["genre_id"], "dim_person": ["person_id"],
        "dim_production_company": ["company_id"], "dim_distributor": ["distributor_key"],
    }
    composite_keys = {
        "bridge_movie_genre": ["imdb_id", "genre_id"],
        "bridge_movie_company": ["imdb_id", "company_id"],
        "bridge_movie_cast_crew": ["imdb_id", "person_id", "role"],
    }
    for table, cols in primary_keys.items():
        frame = tables[table]
        checks[f"{table}: null primary key"] = frame.filter(" OR ".join(f"{c} IS NULL" for c in cols)).count()
        checks[f"{table}: duplicate primary key"] = frame.count() - frame.select(*cols).distinct().count()
    for table, cols in composite_keys.items():
        frame = tables[table]
        checks[f"{table}: null composite key"] = frame.filter(" OR ".join(f"{c} IS NULL" for c in cols)).count()
        checks[f"{table}: duplicate composite key"] = frame.count() - frame.select(*cols).distinct().count()
    for name, bad in checks.items():
        print(("OK   " if bad == 0 else "FAIL ") + name, bad)
    if any(checks.values()):
        raise ValueError("Integrity check failed")


def write_silver(tables, upload=True):
    """Serialize each table to in-memory Parquet and upload it to Azure Silver."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    for name, df in tables.items():
        rows = [row.asDict(recursive=True) for row in df.toLocalIterator()]
        if rows:
            table = pa.Table.from_pylist(rows)
        else:
            table = pa.Table.from_pylist(
                [], schema=pa.schema([pa.field(field.name, pa.null()) for field in df.schema.fields])
            )
        payload = BytesIO()
        pq.write_table(table, payload)
        parquet_bytes = payload.getvalue()
        print("serialized", name + ".parquet", "rows:", len(rows))
        if upload:
            uploaded = upload_bytes_to_adls(
                parquet_bytes,
                name + ".parquet",
                container=AZURE_CONTAINER_SILVER,
                overwrite=True,
            )
            if not uploaded:
                raise RuntimeError(f"Azure Silver upload failed for {name}.parquet")
            print("uploaded to Azure silver:", uploaded)



def run_spark_transformation():
    """Main entry point for the Silver transformation."""
    resolved_inputs = download_bronze_files()
    spark = get_spark()
    movies = None

    try:
        # Read raw source files
        tmdb_raw = spark.read.option("multiLine", True).json(str(resolved_inputs["tmdb_detailed_movies_*.json"])).select(F.explode("movies").alias("m")).select("m.*")
        omdb_raw = spark.read.option("multiLine", True).json(str(resolved_inputs["omdb_movies_*.json"])).select(F.explode("movies").alias("m")).select("m.*")
        bom_raw = spark.read.option("multiLine", True).json(str(resolved_inputs["bom_movies_*.json"])).select(F.explode("movies").alias("m")).select("m.*")

        # Stop early if any required source is missing
        missing = [
            name for name, data in [
                ("TMDb", tmdb_raw),
                ("OMDb", omdb_raw),
                ("Box Office Mojo", bom_raw)
            ]
            if data is None
        ]

        if missing:
            raise FileNotFoundError(
                "Missing required raw data: " + ", ".join(missing)
            )

        # Clean source data
        omdb = clean_omdb(omdb_raw)
        bom = clean_bom(bom_raw)
        movies = clean_tmdb(tmdb_raw).cache()

        # Build dimensions and bridge tables
        dim_genre, bridge_genre = build_genres(movies)
        dim_company, bridge_company = build_companies(movies)
        dim_person, bridge_person = build_people(movies)
        dim_date = build_dim_date(movies)
        fact, dim_distributor = build_fact(movies, omdb, bom)

        # Add the unknown member to every dimension, keeping each schema intact.
        def with_unknown(frame, key_col, values):
            unknown = frame.sparkSession.createDataFrame([values], frame.schema)
            return frame.unionByName(unknown).dropDuplicates([key_col])
        dim_date = with_unknown(dim_date, "date_key", (-1, None, None, None, None, "Unknown", "Unknown"))
        dim_genre = with_unknown(dim_genre, "genre_id", (-1, "Unknown"))
        dim_company = with_unknown(dim_company, "company_id", (-1, "Unknown", "Unknown"))
        dim_person = with_unknown(dim_person, "person_id", (-1, "Unknown", None, "Unknown", None))

        # Collect all Silver tables
        tables = {
            "fact_movies": fact,
            "dim_date": dim_date,
            "dim_distributor": dim_distributor,
            "dim_genre": dim_genre,
            "dim_production_company": dim_company,
            "dim_person": dim_person,
            "bridge_movie_genre": bridge_genre,
            "bridge_movie_company": bridge_company,
            "bridge_movie_cast_crew": bridge_person,
        }
        tables = {name: frame.cache() for name, frame in tables.items()}

        source_counts = {"tmdb": movies.count(), "omdb": omdb.count(), "bom": bom.count()}
        tmdb_ids = movies.select("imdb_id").distinct()
        quality = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_rows": source_counts,
            "join_match_rates": {
                "tmdb_to_omdb": omdb.join(tmdb_ids, "imdb_id", "inner").select("imdb_id").distinct().count() / max(source_counts["tmdb"], 1),
                "tmdb_to_bom": bom.join(tmdb_ids, "imdb_id", "inner").select("imdb_id").distinct().count() / max(source_counts["tmdb"], 1),
                "omdb_to_bom": omdb.join(bom.select("imdb_id"), "imdb_id", "inner").select("imdb_id").distinct().count() / max(source_counts["omdb"], 1),
            },
            "table_rows": {name: frame.count() for name, frame in tables.items()},
        }
        LOGGER.info("Source join match rates: %s", quality["join_match_rates"], extra={"event": "source_join_match_rates"})
        # Display table row counts
        for name, df in tables.items():
            print(name, df.count())

        # Validate relationships
        check_integrity(tables)

        quality["integrity_assertions"] = "passed"
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        (PROCESSED_DIR / "silver_quality_report.json").write_text(json.dumps(quality, indent=2), encoding="utf-8")
        LOGGER.info("Silver quality report written", extra={"event": "silver_quality_report_written"})

        # Save and upload Silver outputs
        write_silver(tables)

        # Display a sample of the fact table
        fact.select(
            "imdb_id",
            "title",
            "budget",
            "worldwide_revenue",
            "profit",
            "roi_percentage",
            "distributor_key",
            "mpaa_rating",
            "imdb_rating"
        ).show(5, truncate=False)

    finally:
        if movies is not None:
            movies.unpersist()

        for frame in locals().get("tables", {}).values():
            frame.unpersist()

        if spark is not None:
            spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":
    run_spark_transformation()
