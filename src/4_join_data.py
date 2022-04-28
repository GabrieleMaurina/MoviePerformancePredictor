from pyspark import SparkContext
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as sqlf
from cpi import inflate
from difflib import SequenceMatcher
from nltk.corpus import stopwords

OUTPUT = 'data/data.tsv'

def main():
    sc = SparkContext('local', '3_join_data')
    sc.setLogLevel('ERROR') #hide useless logging
    spark = SparkSession(sc)

    #title_basics
    title_basics = spark.read.csv('data/title_basics.tsv', sep=r'\t', header=True)
    title_basics = title_basics.withColumn('startYear', title_basics.startYear.cast(IntegerType())) #cast year to int
    title_basics = title_basics.withColumn('runtimeMinutes', title_basics.runtimeMinutes.cast(IntegerType())) #cast runtimeMinutes to int
    title_basics = title_basics.filter(title_basics.titleType == 'movie') #remove non-movies
    title_basics = title_basics.filter(title_basics.isAdult == '0') #remove porn
    title_basics = title_basics.filter(title_basics.runtimeMinutes > 30) #remove short movies
    title_basics = title_basics.filter(title_basics.startYear > 1980) #remove pre-1980 movies
    title_basics = title_basics.filter(title_basics.startYear < 2020) #remove post-2019 movies
    title_basics = title_basics.withColumn('genres', sqlf.split(title_basics.genres, ',')) #split list of genres
    title_basics = title_basics.select('tconst','primaryTitle','startYear','runtimeMinutes','genres') #remove unecessary columns

    #crew
    title_crew = spark.read.csv('data/title_crew.tsv', sep=r'\t', header=True)
    title_crew = title_crew.filter(title_crew.directors != '\\N') #remove movies with no director
    title_crew = title_crew.filter(title_crew.writers != '\\N') #remove movies with no writer
    title_crew = title_crew.withColumn('directors', sqlf.split(title_crew.directors, ',')) #split list of directors
    title_crew = title_crew.withColumn('writers', sqlf.split(title_crew.writers, ',')) #split list of writers

    #title_ratings
    title_ratings = spark.read.csv('data/title_ratings.tsv', sep=r'\t', header=True)
    title_ratings = title_ratings.withColumn('averageRating', title_ratings.averageRating.cast(FloatType())) #cast averageRating to float
    title_ratings = title_ratings.withColumn('numVotes', title_ratings.numVotes.cast(IntegerType())) #cast numVotes to int

    #title_principals
    title_principals = spark.read.csv('data/title_principals.tsv', sep=r'\t', header=True)
    title_principals = title_principals.filter(title_principals.category != 'self') #remove 'self'
    title_principals = title_principals.select('tconst', 'nconst', 'category') #remove unecessary columns

    #encode people
    people = title_principals.join(title_ratings, 'tconst') #join ratings
    n_titles = sqlf.count('tconst').alias('n_titles') #count titles
    average_rating = sqlf.avg('averageRating').alias('average_rating') #compute average rating
    max_rating = sqlf.max('averageRating').alias('max_rating') #compute max rating
    median_rating = sqlf.percentile_approx('averageRating', 0.5).alias('median_rating') #compute median rating
    people = people.groupby('nconst').agg(n_titles, average_rating, max_rating, median_rating) #compute metrics

    #title_akas
    title_akas = spark.read.csv('data/title_akas.tsv', sep=r'\t', header=True)
    title_akas = title_akas.withColumnRenamed('titleId', 'tconst') #rename titleId to tconst as in the other tables
    title_akas = title_akas.groupBy('tconst').count() #count how many releases
    title_akas = title_akas.withColumnRenamed('count', 'releases') #rename count to releases

    #scraped
    scraped = spark.read.csv('data/scraped.tsv', sep=r'\t', header=True)
    scraped = scraped.withColumn('box_office', scraped.box_office.cast(IntegerType())) #cast box_office to int
    scraped = scraped.withColumn('budget', scraped.budget.cast(IntegerType())) #cast budget to int
    scraped = scraped.withColumn('audience_score', scraped.audience_score.cast(IntegerType())) #cast audience_score to int
    scraped = scraped.withColumn('critics_score', scraped.critics_score.cast(IntegerType())) #cast critics_score to int
    scraped = scraped.na.drop() #remove rows with null values

    #joins
    data = scraped.join(title_basics, 'tconst')
    data = data.join(title_crew, 'tconst')
    data = data.join(title_ratings, 'tconst')
    data = data.join(title_akas, 'tconst')

    #adjust for inflation
    adjust = sqlf.udf(lambda value, year: int(inflate(value, year))) #cread udf function for adjustment
    data = data.withColumn('budget', adjust(data.budget, data.startYear)) #adjust budget
    data = data.withColumn('box_office', adjust(data.box_office, data.startYear)) #adjust boxoffice

    #encoding genres
    GENRES = ('Action', 'Adult', 'Adventure', 'Animation', 'Biography',
        'Comedy','Crime', 'Documentary', 'Drama', 'Family', 'Fantasy', 'History',
        'Horror', 'Musical', 'Music', 'Mystery', 'News', 'Reality-TV', 'Romance',
        'Sci-Fi', 'Sport', 'Talk-Show', 'Thriller', 'War', 'Western')
    for genre in GENRES:
        data = data.withColumn(genre, sqlf.when(sqlf.array_contains('genres', genre), 1).otherwise(0)) #add a column for each genre
    data = data.drop('genres') #remove genres column

    #encode franchise
    STOPWORDS = set(stopwords.words('english')) #collect English stopwords

    def process_title(title):
        #remove stopwords from title
        return ' '.join(filter(lambda w: w not in STOPWORDS, title.lower().split()))
    process_title = sqlf.udf(process_title) #create udf function to process titles

    def sim(title1, title2):
        #check if titles are similar
        matcher = SequenceMatcher(None, title1, title2)
        ratio = matcher.ratio()
        match_size = matcher.find_longest_match(0,len(title1),0,len(title2)).size
        return ratio > 0.9 or (ratio > 0.7 and match_size > 7) or match_size > 12
    sim = sqlf.udf(sim) #create udf function to check title similarity

    def set_union(l):
        s = set()
        s.update(*l)
        return s
    set_union = sqlf.udf(set_union) #create udf function to join sets

    titles = data.select('tconst', 'primaryTitle') # get list of title
    titles = titles.withColumn('primaryTitle', process_title(titles.primaryTitle)) #remove stopwords
    titles1 = titles.withColumnRenamed('tconst', 'tconst1').withColumnRenamed('primaryTitle', 'title1') #create titles1
    titles2 = titles.withColumnRenamed('tconst', 'tconst2').withColumnRenamed('primaryTitle', 'title2') #create titles2
    franchise = titles1.join(titles2) #create pairs of titles
    franchise = franchise.where(franchise.tconst1 < franchise.tconst2) #remove duplicates
    franchise = franchise.withColumn('edge', sim(franchise.title1, franchise.title2)) #compute edges
    franchise = franchise.where(franchise.edge == True) #keep only pairs with edge
    franchise = franchise.select('tconst1', 'tconst2') #keep only pair of ids
    neighbors1 = franchise.groupBy('tconst1').agg(sqlf.collect_set('tconst2').alias('neighbors')) #compute neighbors1
    neighbors2 = franchise.groupBy('tconst2').agg(sqlf.collect_set('tconst1').alias('neighbors')) #compute neighbors2
    neighbors1 = neighbors1.withColumnRenamed('tconst1', 'tconst') #rename id
    neighbors2 = neighbors2.withColumnRenamed('tconst2', 'tconst') #rename id
    franchise = neighbors1.union(neighbors2) #union neighbors dataframes
    franchise = franchise.groupBy('tconst').agg(sqlf.collect_list('neighbors').alias('neighbors')) #collect sets of neighbors
    franchise = franchise.withColumn('neighbors', set_union(franchise.neighbors)) #union all sets of neighbors
    franchise = franchise.collect()

    #addition for principals (Azlan 4/25/2022)
    title_crew = title_crew.withColumn("directors", concat_ws(",",col("directors")))
    title_crew = title_crew.withColumn("writers", concat_ws(",",col("writers")))

    title_crew = title_crew.withColumn('director_1', split(title_crew['directors'], ',').getItem(0)) \
           .withColumn('director_2', split(title_crew['directors'], ',').getItem(1))

    title_crew = title_crew.withColumn('writer_1', split(title_crew['writers'], ',').getItem(0)) \
           .withColumn('writer_2', split(title_crew['writers'], ',').getItem(1)) \
           .withColumn('writer_3', split(title_crew['writers'], ',').getItem(2))

    title_crew = title_crew.withColumn('writer_1', concat(col('writer_1'), lit(',writer,'), col('tconst'))) \
           .withColumn('writer_2', concat(col('writer_2'), lit(',writer,'), col('tconst'))) \
           .withColumn('writer_3', concat(col('writer_3'), lit(',writer,'), col('tconst')))

    title_crew = title_crew.withColumn('director_1', concat(col('director_1'), lit(',director,'), col('tconst'))) \
         .withColumn('director_2', concat(col('director_2'), lit(',director,'), col('tconst')))

    crew_final = title_crew.select(explode(array(
            col("director_1"),
            col("director_2"),
            col("writer_1"),
            col("writer_2"),
            col("writer_3")))).distinct()

    crew_final = crew_final.withColumn('tconst', split(crew_final['col'], ',').getItem(2)) \
          .withColumn('nconst', split(crew_final['col'], ',').getItem(0)) \
          .withColumn('category', split(crew_final['col'], ',').getItem(1)).select('tconst', 'nconst', 'category')

    df = title_principals.join(title_ratings, 'tconst')
    df2 = crew_final.join(title_ratings, 'tconst')
    principals1 = df2.groupby('nconst').agg(countDistinct('tconst')).alias('titles_ct')
    principals2 = df2.groupby('nconst').agg(avg('averageRating')).alias('avg_rating')
    principals3 = df2.groupby('nconst').agg(max('averageRating')).alias('max_rating')
    principals4 = df2.groupby('nconst').agg(func.percentile_approx("averageRating", 0.5)).alias('medn_rating')
    principals_df1 = principals1.join(principals2, 'nconst').join(principals3, 'nconst').join(principals4, 'nconst')

    principals1 = df1.groupby('nconst').agg(countDistinct('tconst')).alias('titles_ct')
    principals2 = df1.groupby('nconst').agg(avg('averageRating')).alias('avg_rating')
    principals3 = df1.groupby('nconst').agg(max('averageRating')).alias('max_rating')
    principals4 = df1.groupby('nconst').agg(func.percentile_approx("averageRating", 0.5)).alias('medn_rating')
    principals_df2 = principals1.join(principals2, 'nconst').join(principals3, 'nconst').join(principals4, 'nconst')

    principals = principals_df1.union(principals_df2)
    principals.show()


    # Encoding cast and crew (directors, writers, actors)
    #for i in new_6.select("category").distinct().collect():
    #    new_6 = new_6.withColumn(i, when(new_6['category'].contains(i), nconst).otherwise(0))
    #new_6.show()

if __name__ == '__main__':
    main()
