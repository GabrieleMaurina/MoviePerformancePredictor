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
    people = people.join(title_principals, 'nconst') #join with movies

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
    
    def replace_none(col, v=0):
        global data
        data = data.withColumn(col, sqlf.coalesce(sqlf.col(col), sqlf.lit(v)))
    
    replace_none('franchise_titles', 1.0)
    replace_none('franchise_average_rating')
    replace_none('franchise_max_rating')
    replace_none('franchise_median_rating')
    replace_none('franchise_average_box_office')
    replace_none('franchise_max_box_office')
    replace_none('franchise_median_box_office')

    #encoding cast and crew
    def rename_cols(df, old, new):
        df = df.withColumnRenamed(f'{old}_n_titles', f'{new}_n_titles')
        df = df.withColumnRenamed(f'{old}_avg_rating', f'{new}_avg_rating')
        df = df.withColumnRenamed(f'{old}_max_rating', f'{new}_max_rating')
        df = df.withColumnRenamed(f'{old}_median_rating', f'{new}_median_rating')
        return df.drop('nconst')

    def encode_category(category, how_many):
        global data
        workers = people.where(people.category == category)
        workers = workers.select(sqlf.col('n_titles').alias(f'{category}_n_titles'),
                                sqlf.col('average_rating').alias(f'{category}_avg_rating'),
                                sqlf.col('max_rating').alias(f'{category}_max_rating'),
                                sqlf.col('median_rating').alias(f'{category}_median_rating'),
                                'nconst', 'tconst')
        all_next_workers = workers
        for i in range(how_many):
            next_workers = all_next_workers.dropDuplicates(['tconst'])
            all_next_workers = all_next_workers.join(next_workers, 'nconst', 'leftanti')
            next_workers = rename_cols(next_workers, category, f'{category}_{i}')
            data = data.join(next_workers, 'tconst', 'left')
            replace_none(f'{category}_{i}_n_titles')
            replace_none(f'{category}_{i}_avg_rating')
            replace_none(f'{category}_{i}_max_rating')
            replace_none(f'{category}_{i}_median_rating')

    encode_category('director', 2)
    encode_category('writer', 3)
    encode_category('actor', 2)
    encode_category('actress', 2)
    encode_category('cinetographer', 2)
    encode_category('producer', 2)

    #save data
    data = data.sort(data.startYear)
    save_table(data, 'data/data.tsv')

if __name__ == '__main__':
    main()
