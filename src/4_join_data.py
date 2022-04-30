from pyspark import SparkContext
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as sqlf
from cpi import inflate
from difflib import SequenceMatcher
from nltk.corpus import stopwords

def save_table(dataframe, path, header=True, separator='\t'):
    table = dataframe.collect()
    if header:
        table.insert(0, dataframe.columns)
    with open(path, 'w') as out:
        for row in table:
            out.write(separator.join(str(value) for value in row) + '\n')

def main():
    sc = SparkContext('local', '4_join_data')
    sc.setLogLevel('ERROR') #hide useless logging
    spark = SparkSession(sc)

    #title_basics
    title_basics = spark.read.csv('data/title_basics.tsv', sep=r'\t', header=True)
    title_basics = title_basics.withColumn('startYear', title_basics.startYear.cast(IntegerType())) #cast year to int
    title_basics = title_basics.withColumn('runtimeMinutes', title_basics.runtimeMinutes.cast(IntegerType())) #cast runtimeMinutes to int
    title_basics = title_basics.filter(title_basics.titleType == 'movie') #remove non-movies
    title_basics = title_basics.filter(title_basics.isAdult == '0') #remove porn
    title_basics = title_basics.filter(title_basics.runtimeMinutes > 30) #remove short movies
    title_basics = title_basics.filter(title_basics.startYear > 1969) #remove pre-1970 movies
    title_basics = title_basics.filter(title_basics.startYear < 2020) #remove post-2019 movies
    title_basics = title_basics.withColumn('genres', sqlf.split(title_basics.genres, ',')) #split list of genres
    title_basics = title_basics.select('tconst','primaryTitle','startYear','runtimeMinutes','genres') #remove unecessary columns

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
    people = people.join(title_principals, 'nconst')

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

    #franchises
    franchises = spark.read.csv('data/franchises.tsv', sep=r'\t', header=True)
    franchises_data = franchises.join(data, 'tconst') #join with data
    franchises_data = franchises_data.select('franchise', 'tconst', 'averageRating', 'box_office') #remove unecessary columns

    franchise_titles = sqlf.count('tconst').alias('franchise_titles') #count titles
    franchise_average_rating = sqlf.avg('averageRating').alias('franchise_average_rating') #compute average rating
    franchise_max_rating = sqlf.max('averageRating').alias('franchise_max_rating') #compute max rating
    franchise_median_rating = sqlf.percentile_approx('averageRating', 0.5).alias('franchise_median_rating') #compute median rating

    franchise_average_box_office = sqlf.avg('box_office').alias('franchise_average_box_office') #compute average box office
    franchise_max_box_office = sqlf.max('box_office').alias('franchise_max_box_office') #compute max box office
    franchise_median_box_office = sqlf.percentile_approx('box_office', 0.5).alias('franchise_median_box_office') #compute median box office

    franchises_data = franchises_data.groupBy('franchise') #group by franchise
    franchises_data = franchises_data.agg(franchise_titles, franchise_average_rating, franchise_max_rating, franchise_median_rating, franchise_average_box_office, franchise_max_box_office, franchise_median_box_office) #compute metrics
    data = data.join(franchises, 'tconst', 'left').join(franchises_data, 'franchise', 'left') #join data and franchises
    data = data.drop('franchise') #remove franchise id
    data = data.withColumn('franchise_titles', sqlf.coalesce(data.franchise_titles, sqlf.lit(1.0))) #franchise_titles default value 1
    data = data.withColumn('franchise_average_rating', sqlf.coalesce(data.franchise_average_rating, sqlf.lit(0.0))) #franchise_average_rating default value 0
    data = data.withColumn('franchise_max_rating', sqlf.coalesce(data.franchise_max_rating, sqlf.lit(0.0))) #franchise_max_rating default value 0
    data = data.withColumn('franchise_median_rating', sqlf.coalesce(data.franchise_median_rating, sqlf.lit(0.0))) #franchise_median_rating default value 0
    data = data.withColumn('franchise_average_box_office', sqlf.coalesce(data.franchise_average_box_office, sqlf.lit(0.0))) #franchise_average_box_office default value 0
    data = data.withColumn('franchise_max_box_office', sqlf.coalesce(data.franchise_max_box_office, sqlf.lit(0.0))) #franchise_max_box_office default value 0
    data = data.withColumn('franchise_median_box_office', sqlf.coalesce(data.franchise_median_box_office, sqlf.lit(0.0))) #franchise_median_box_office default value 0

    # Encoding cast and crew (Eric 4/29/2022)
    def renameCols(df, oldstring, newstring):
        df = df.withColumnRenamed(f'{oldstring} n_titles', f'{newstring} n_titles')
        df = df.withColumnRenamed(f'{oldstring} avg. rating', f'{newstring} avg. rating')
        df = df.withColumnRenamed(f'{oldstring} max. rating', f'{newstring} max. rating')
        df = df.withColumnRenamed(f'{oldstring} med. rating', f'{newstring} med. rating')

        df = df.drop('nconst')

        return df


    directors = people.filter(people['category'] == 'director').select(sqlf.col('n_titles').alias('director n_titles'),
                                                                       sqlf.col('average_rating').alias('director avg. rating'),
                                                                       sqlf.col('max_rating').alias('director max. rating'),
                                                                       sqlf.col('median_rating').alias('director med. rating'),
                                                                       'nconst', 'tconst')
    directors_unique = directors.dropDuplicates(['tconst'])
    directors2 = directors.join(directors_unique, 'nconst', 'leftanti')
    directors2_unique = directors2.dropDuplicates(['tconst'])
    directors3 = directors2.join(directors2_unique, 'nconst', 'leftanti').dropDuplicates()
    directors2 = renameCols(directors2, 'director', 'director 2')
    directors3 = renameCols(directors3, 'director', 'director 3')
    data = data.join(directors, 'tconst', 'left')
    data = data.join(directors2, 'tconst', 'left')
    data = data.join(directors3, 'tconst', 'left')


    writers = people.filter(people['category'] == 'writer').select(sqlf.col('n_titles').alias('writer n_titles'),
                                                                       sqlf.col('average_rating').alias('writer avg. rating'),
                                                                       sqlf.col('max_rating').alias('writer max. rating'),
                                                                       sqlf.col('median_rating').alias('writer med. rating'),
                                                                       'nconst', 'tconst')
    writers_unique = writers.dropDuplicates(['tconst'])
    writers2 = writers.join(writers_unique, 'nconst', 'leftanti')
    writers2_unique = writers2.dropDuplicates(['tconst'])
    writers3 = writers2.join(writers2_unique, 'nconst', 'leftanti').dropDuplicates()
    writers2 = renameCols(writers2, 'writer', 'writer 2')
    writers3 = renameCols(writers3, 'writer', 'writer 3')
    data = data.join(writers, 'tconst', 'left')
    data = data.join(writers, 'tconst', 'left')
    data = data.join(writers, 'tconst', 'left')


    actors = people.filter(people['category'] == 'actor').select(sqlf.col('n_titles').alias('actor n_titles'),
                                                                       sqlf.col('average_rating').alias('actor avg. rating'),
                                                                       sqlf.col('max_rating').alias('actor max. rating'),
                                                                       sqlf.col('median_rating').alias('actor med. rating'),
                                                                       'nconst', 'tconst')
    actors_unique = actors.dropDuplicates(['tconst'])
    actors2 = actors.join(actors_unique, 'nconst', 'leftanti')
    actors2_unique = actors2.dropDuplicates(['tconst'])
    actors3 = actors2.join(actors2_unique, 'nconst', 'leftanti').dropDuplicates()
    actors2 = renameCols(actors2, 'actor', 'actor 2')
    actors3 = renameCols(actors3, 'actor', 'actor 3')
    data = data.join(actors, 'tconst', 'left')
    data = data.join(actors, 'tconst', 'left')
    data = data.join(actors, 'tconst', 'left')


    composers = people.filter(people['category'] == 'composer').select(sqlf.col('n_titles').alias('composer n_titles'),
                                                                       sqlf.col('average_rating').alias('composer avg. rating'),
                                                                       sqlf.col('max_rating').alias('composer max. rating'),
                                                                       sqlf.col('median_rating').alias('composer med. rating'),
                                                                       'nconst', 'tconst')
    composers_unique = composers.dropDuplicates(['tconst'])
    composers2 = composers.join(composers_unique, 'nconst', 'leftanti')
    composers2_unique = composers2.dropDuplicates(['tconst'])
    composers3 = composers2.join(composers2_unique, 'nconst', 'leftanti').dropDuplicates()
    composers2 = renameCols(composers2, 'composer', 'composer 2')
    composers3 = renameCols(composers3, 'composer', 'composer 3')
    data = data.join(composers, 'tconst', 'left')
    data = data.join(composers, 'tconst', 'left')
    data = data.join(composers, 'tconst', 'left')

    cinemetographers = people.filter(people['category'] == 'cinetographer').select(sqlf.col('n_titles').alias('cinemetographer n_titles'),
                                                                       sqlf.col('average_rating').alias('cinemetographer avg. rating'),
                                                                       sqlf.col('max_rating').alias('cinemetographer max. rating'),
                                                                       sqlf.col('median_rating').alias('cinemetographer med. rating'),
                                                                       'nconst', 'tconst')
    cinemetographers_unique = cinemetographers.dropDuplicates(['tconst'])
    cinemetographers2 = cinemetographers.join(cinemetographers_unique, 'nconst', 'leftanti')
    cinemetographers2_unique = cinemetographers2.dropDuplicates(['tconst'])
    cinemetographers3 = cinemetographers2.join(cinemetographers2_unique, 'nconst', 'leftanti').dropDuplicates()
    cinemetographers2 = renameCols(cinemetographers2, 'cinemetographer', 'cinemetographer 2')
    cinemetographers3 = renameCols(cinemetographers3, 'cinemetographer', 'cinemetographer 3')
    data = data.join(cinemetographers, 'tconst', 'left')
    data = data.join(cinemetographers, 'tconst', 'left')
    data = data.join(cinemetographers, 'tconst', 'left')



    producers = people.filter(people['category'] == 'producer').select(sqlf.col('n_titles').alias('producer n_titles'),
                                                                       sqlf.col('average_rating').alias('producer avg. rating'),
                                                                       sqlf.col('max_rating').alias('producer max. rating'),
                                                                       sqlf.col('median_rating').alias('producer med. rating'),
                                                                       'nconst', 'tconst')
    producers_unique = producers.dropDuplicates(['tconst'])
    producers2 = producers.join(producers_unique, 'nconst', 'leftanti')
    producers2_unique = producers2.dropDuplicates(['tconst'])
    producers3 = producers2.join(producers2_unique, 'nconst', 'leftanti').dropDuplicates()
    producers2 = renameCols(producers2, 'producer', 'producer 2')
    producers3 = renameCols(producers3, 'producer', 'producer 3')
    data = data.join(producers, 'tconst', 'left')
    data = data.join(producers, 'tconst', 'left')
    data = data.join(producers, 'tconst', 'left')


    #save data
    data = data.sort(data.startYear)
    save_table(data, 'data/data.tsv')

if __name__ == '__main__':
    main()
