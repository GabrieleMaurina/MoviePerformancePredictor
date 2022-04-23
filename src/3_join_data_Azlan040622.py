from pyspark import SparkContext
from pyspark.sql.types import IntegerType
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import when
import pyspark.sql.functions as F

OUTPUT = 'data/data.tsv'

def tsv(v):
    return v.split('\t')

def tconst(v):
    return v[0] != 'tconst'

def nconst(v):
    return v[0] != 'nconst'

def titleId(v):
    return v[0] != 'titleId'

def main():
    sc = SparkContext('local', '3_join_data')
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    movies = sc.textFile('/content/data/movies.tsv').map(tsv).filter(tconst).map(lambda v: v[0])
    basics = sc.textFile('/content/data/title.basics.tsv').map(tsv).filter(tconst).map(lambda v: (v[0], (v[2], v[5], v[7], v[8])))
    akas = sc.textFile('/content/data/title.akas.tsv').map(tsv).filter(titleId)
    crew = sc.textFile('/content/data/title.crew.tsv').map(tsv).filter(tconst).map(lambda v: (v[0], (v[1], v[2])))
    principals = sc.textFile('/content/data/title.principals.tsv').map(tsv).filter(tconst)
    rating = sc.textFile('/content/data/title.rating.tsv').map(tsv).filter(tconst)
    name = sc.textFile('/content/data/name.basics.tsv').map(tsv).filter(nconst)
    scraped = sc.textFile('/content/data/scraped.tsv').map(tsv)


    # Join all IMDB data files, filter for columns and drop columns
    basics_2 = spark.read.csv('title_basics.tsv', sep=r'\t', header=True).select('tconst','primaryTitle','startYear','runTimeMinutes','genres')
    basics_2 = basics_2.withColumn("runTimeMinutes", basics_2["runTimeMinutes"].cast(IntegerType())).filter(basics_2.runTimeMinutes > 30) # filtering out movies less than 30 minutes
    movies_2 = spark.read.csv('movies.tsv', sep=r'\t', header=True).select('tconst').selectExpr("tconst as tconst1")
    new = movies_2.join(basics_2, basics_2.tconst == movies_2.tconst1, "inner").drop("tconst1")
    basics_2.unpersist()
    movies_2.unpersist()
    crew_2 = spark.read.csv('title_crew.tsv', sep=r'\t', header=True).select('tconst','directors','writers').selectExpr("tconst as tconst1",'directors as directors', 'writers as writers')
    new_2 = new.join(crew_2, new.tconst == crew_2.tconst1, "inner").drop("tconst1")
    crew_2.unpersist()
    new.unpersist()
    ratings_2 = spark.read.csv('title_ratings.tsv', sep=r'\t', header=True).select('tconst','averageRating','numVotes').selectExpr("tconst as tconst1",'averageRating as averageRating', 'numVotes as numVotes')
    new_3 = new_2.join(ratings_2, new_2.tconst == ratings_2.tconst1, "inner").drop("tconst1")
    ratings_2.unpersist()
    new_2.unpersist()
    principals_2 = spark.read.csv('title_principals.tsv', sep=r'\t', header=True).select('tconst','nconst','category','job', 'characters').selectExpr("tconst as tconst1",'nconst as nconst', 'category as category', 'job as job', 'characters as characters')
    new_4 = new_3.join(principals_2, new_3.tconst == principals_2.tconst1, "inner").drop("tconst1")
    principals_2.unpersist()
    new_3.unpersist()
    scr = spark.read.csv('scraped.tsv', sep=r'\t', header=True).select('tconst', 'box_office', 'budget', 'audience_score', 'critics_score').selectExpr('tconst as tconst1', 'box_office as box_office', 'budget as budget', 'audience_score as audience_score', 'critics_score as critics_score')
    
    # Join scraped data from BoxOfficeMojo and Rotten Tomatoes
    scr = spark.read.csv('scraped.tsv', sep=r'\t', header=True).select('tconst', 'box_office', 'budget', 'audience_score', 'critics_score').selectExpr('tconst as tconst1', 'box_office as box_office', 'budget as budget', 'audience_score as audience_score', 'critics_score as critics_score')
    new_5 = new_4.join(scr, new_4.tconst == scr.tconst1, "inner").drop("tconst1")
    scr.unpersist()
    new_4.unpersist()
    new_5 = new_5.withColumn("startYear", new_5["startYear"].cast(IntegerType())).filter(new_5.startYear >= 1990) # filtering out movies before 1990 (remaining 158K movies)
    
    # Drop movies with no associated RT or BoxOffice records
    new_5 = new_5.na.drop(subset=["audience_score","critics_score","box_office"]).show(truncate=False)
    
    # Encoding genres
    gen = ["Action", "Adult", "Adventure", "Animation", "Biography", "Comedy", "Crime", "Documentary", "Drama", "Family", 
    "Fantasy", "FilmNoir", "GameShow", "History", "Horror", "Musical", "Music", "Mystery", "News", "Reality-TV", 
    "Romance", "Sci-Fi", "Short", "Sport", "Talk-Show", "Thriller", "War", "Western"]
    for i in gen:
        new_5 = new_5.withColumn(i, when(new_5['genres'].contains(i), 1).otherwise(0))
    #new_5.show()
    
    # Encoding cast and crew (directors, writers, actors)
    nconst_en = new_5.select("nconst").distinct().rdd.flatMap(lambda x: x).collect()
    crew = [F.when(F.col("nconst") == cr, 1).otherwise(0).alias("nconst_" + cr) for cr in nconst_en]
    new_6 = new_5.select("tconst", "primaryTitle", "startYear", "runTimeMinutes", "averageRating", "numVotes", "box_office", "budget", "audience_score", "critics_score", *crew)
    new_6.head(10)

if __name__ == '__main__':
    main()
