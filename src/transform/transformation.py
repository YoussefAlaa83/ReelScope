import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from pyspark.sql.types import IntegerType, LongType

from src.utils.azure_storage import upload_file_to_adls
from src.utils.config import AZURE_CONTAINER_SILVER, DATA_DIR, RAW_DATA_DIR

SILVER_DIR = DATA_DIR / "silver"


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
    w = Window.partitionBy("imdb_id").orderBy(F.col("vote_count").desc())
    df = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    for c in ["budget", "revenue", "runtime"]:
        df = df.withColumn(c, F.when(F.col(c) > 0, F.col(c)))        # 0 means unknown -> null
    return df.withColumn("release_date", F.to_date("release_date", "yyyy-MM-dd"))


def build_genres(movies):
    """dim_genre + bridge_movie_genre from the nested genres list."""
    g = movies.select("imdb_id", F.explode("genres").alias("g"))
    bridge = g.select("imdb_id", F.col("g.id").alias("genre_id")).dropDuplicates()
    dim = (g.select(F.col("g.id").alias("genre_id"), F.col("g.name").alias("genre_name"))
            .dropDuplicates(["genre_id"]))
    return dim, bridge


def build_companies(movies):
    """dim_company + bridge_movie_company from the nested production_companies list."""
    c = movies.select("imdb_id", F.explode("production_companies").alias("c"))
    bridge = c.select("imdb_id", F.col("c.id").alias("company_id")).dropDuplicates()
    dim = (c.select(F.col("c.id").alias("company_id"), F.col("c.name").alias("company_name"),
                    F.col("c.origin_country").alias("origin_country"))
            .dropDuplicates(["company_id"]))
    return dim, bridge

def build_people(movies):
    """dim_person + bridge_movie_person: top-5 billed actors and directors."""
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
    dim = (people.select("person_id", "person_name", "gender", "known_for_department", "popularity")
           .dropDuplicates(["person_id"]))
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
    return F.regexp_replace(F.col(col), "[^0-9.]", "").cast("double")


def clean_omdb(omdb):
    """OMDb: strings -> proper types, one row per imdb_id."""
    rotten = F.filter(F.col("Ratings"), lambda r: r["Source"] == "Rotten Tomatoes").getItem(0).getField("Value")
    return (omdb.select(
        F.trim(F.lower(F.col("imdbID"))).alias("imdb_id"),
        na("Rated").alias("omdb_rated"),
        na("Awards").alias("awards_summary"),
        na("Poster").alias("poster_url"),
        num("Metascore").cast("int").alias("metascore"),
        num("imdbRating").alias("imdb_rating"),
        num("imdbVotes").cast("long").alias("imdb_votes"),
        num("BoxOffice").cast("long").alias("omdb_boxoffice"),
        F.regexp_replace(rotten, "%", "").cast("int").alias("rotten_tomatoes_score"),
        F.regexp_extract(F.col("Awards"), r"Won (\d+) Oscar", 1).cast("int").alias("oscar_wins"),
        F.regexp_extract(F.col("Awards"), r"(\d+) wins?", 1).cast("int").alias("total_wins"))
        .dropDuplicates(["imdb_id"]))


def clean_bom(bom):
    """Box Office Mojo: money columns, 0 -> null, one row per imdb_id."""
    def money(col):
        c = F.col(col).cast("long")
        return F.when(c > 0, c)

    return (bom.select(
        F.trim(F.lower(F.col("imdb_id"))).alias("imdb_id"),
        money("domestic_revenue").alias("bom_domestic"),
        money("international_revenue").alias("bom_international"),
        money("worldwide_revenue").alias("bom_worldwide"),
        money("opening_weekend").alias("opening_weekend"),
        na("distributor").alias("distributor"),
        na("mpaa_rating").alias("bom_mpaa"))
        .dropDuplicates(["imdb_id"]))

def build_dim_distributor(df):
    """One row per distributor ('Unknown' when missing) with a surrogate key."""
    names = df.select(F.coalesce(F.col("distributor"), F.lit("Unknown")).alias("distributor_name")).distinct()
    w = Window.orderBy("distributor_name")
    return (names.withColumn("distributor_key", F.row_number().over(w))
            .select("distributor_key", "distributor_name"))


def build_fact(movies, omdb, bom):
    """fact_movies: one row per movie, TMDb + OMDb + Mojo combined, with profit and ROI."""
    df = (movies.join(omdb, "imdb_id", "left").join(bom, "imdb_id", "left")
          .withColumn("distributor_name", F.coalesce(F.col("distributor"), F.lit("Unknown"))))
    dim_distributor = build_dim_distributor(df)
    df = df.join(dim_distributor, "distributor_name", "left")

    budget = F.col("budget").cast("long")
    worldwide = F.coalesce(F.col("bom_worldwide"), F.col("revenue").cast("long"))   # Mojo first, TMDb as fallback
    domestic = F.col("bom_domestic")
    international = F.coalesce(F.col("bom_international"),
                               F.when(worldwide.isNotNull() & domestic.isNotNull(), worldwide - domestic))

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
        F.when((budget > 0) & (worldwide > 0), worldwide - budget).alias("profit"),
        F.when(budget > 0, F.round((worldwide - budget) / budget * 100, 2)).alias("roi_percentage"),
        F.col("runtime").cast("int").alias("runtime_minutes"),
        F.col("vote_average").alias("tmdb_rating"),
        F.col("vote_count").alias("tmdb_votes"),
        "imdb_rating", "imdb_votes", "metascore", "rotten_tomatoes_score", "popularity")
    return fact, dim_distributor


def check_integrity(tables):
    """Every bridge/foreign key must resolve; imdb_id must be unique and not null."""
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
        "company bridge -> dim_company": tables["bridge_movie_company"].join(tables["dim_company"], "company_id", "left_anti").count(),
        "person bridge -> fact": tables["bridge_movie_person"].join(fact, "imdb_id", "left_anti").count(),
        "person bridge -> dim_person": tables["bridge_movie_person"].join(tables["dim_person"], "person_id", "left_anti").count(),
    }
    for name, bad in checks.items():
        print(("OK   " if bad == 0 else "FAIL ") + name, bad)
    if any(checks.values()):
        raise ValueError("Integrity check failed")

def write_silver(tables, upload=True):
    """Write each table as ONE .parquet file in data/silver/ and upload it to Azure silver."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        pdf = df.toPandas()
        for field in df.schema.fields:                       # keep integer columns as integers (with nulls)
            if isinstance(field.dataType, (IntegerType, LongType)):
                pdf[field.name] = pdf[field.name].astype("Int64")
        path = SILVER_DIR / (name + ".parquet")
        pdf.to_parquet(str(path), index=False)
        print("saved", path.name, len(pdf))
        if upload:
            ok = upload_file_to_adls(path, name + ".parquet", container=AZURE_CONTAINER_SILVER)
            print("   uploaded to Azure silver:", ok)

def run_spark_transformation():
    """Main entry point (Airflow will call this)."""
    spark = get_spark()
    tmdb = read_movies(spark, "tmdb_detailed_movies_*.json")
    omdb = clean_omdb(read_movies(spark, "omdb_movies_*.json"))
    bom = clean_bom(read_movies(spark, "bom_movies_*.json"))
    movies = clean_tmdb(tmdb).cache()

    dim_genre, bridge_genre = build_genres(movies)
    dim_company, bridge_company = build_companies(movies)
    dim_person, bridge_person = build_people(movies)
    dim_date = build_dim_date(movies)
    fact, dim_distributor = build_fact(movies, omdb, bom)

    tables = {
        "fact_movies": fact, "dim_date": dim_date, "dim_distributor": dim_distributor,
        "dim_genre": dim_genre, "bridge_movie_genre": bridge_genre,
        "dim_company": dim_company, "bridge_movie_company": bridge_company,
        "dim_person": dim_person, "bridge_movie_person": bridge_person,
    }
    for name, df in tables.items():
        print(name, df.count())
    check_integrity(tables)
    write_silver(tables)
    fact.select("imdb_id", "title", "budget", "worldwide_revenue", "profit",
                "roi_percentage", "distributor_key", "mpaa_rating", "imdb_rating").show(5, truncate=False)
    spark.stop()

if __name__ == "__main__":
    run_spark_transformation()