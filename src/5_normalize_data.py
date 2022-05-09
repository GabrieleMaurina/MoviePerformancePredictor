from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as sqlf
from pyspark.sql.types import FloatType

BOX_OFFICE = (0, 2000000000)
AVERAGE_RATING = (5, 9)

INTERVALS = {
    'startYear': (1970, 2019),
    'box_office': BOX_OFFICE,
    'budget': (100000, 200000000),
    'audience_score': (50, 100),
    'critics_score': (50, 100),
    'runtimeMinutes': (30, 180),
    'averageRating': AVERAGE_RATING,
    'numVotes': (10000, 1000000),
    'releases': (1, 60),
    'franchise_titles': (1, 5),
    'franchise_average_rating': AVERAGE_RATING,
    'franchise_max_rating': AVERAGE_RATING,
    'franchise_median_rating': AVERAGE_RATING,
    'franchise_average_box_office': BOX_OFFICE,
    'franchise_max_box_office': BOX_OFFICE,
    'franchise_median_box_office': BOX_OFFICE
}

def add_crew_interval(category, how_many):
    for i in range(how_many):
        INTERVALS[f'{category}_{i}_n_titles'] = AVERAGE_RATING
        INTERVALS[f'{category}_{i}_avg_rating'] = AVERAGE_RATING
        INTERVALS[f'{category}_{i}_max_rating'] = AVERAGE_RATING
        INTERVALS[f'{category}_{i}_median_rating'] = AVERAGE_RATING

add_crew_interval('director', 2)
add_crew_interval('writer', 3)
add_crew_interval('actor', 2)
add_crew_interval('actress', 2)
add_crew_interval('cinetographer', 2)
add_crew_interval('producer', 2)

def save_table(dataframe, path, header=True, separator='\t'):
    table = dataframe.collect()
    if header:
        table.insert(0, dataframe.columns)
    with open(path, 'w') as out:
        for row in table:
            out.write(separator.join(str(value) for value in row) + '\n')

def normalize(a, b): #normalize uses currying for pickling problems
    def normilizer(v):
        v = (v - a) / (b - a)
        if v < 0.0: v = 0.0 # no less than 0.0
        elif v > 1.0: v = 1.0  # no more than 1.0
        return v
    return sqlf.udf(normilizer)

def main():
    sc = SparkContext('local', '5_normalize_data')
    sc.setLogLevel('ERROR') #hide useless logging
    spark = SparkSession(sc)

    normalized_data = spark.read.csv('data/data.tsv', sep=r'\t', header=True)
    for column, interval in INTERVALS.items(): #for all the columns to normalize
        normalized_data = normalized_data.withColumn(column, sqlf.col(column).cast(FloatType())) #cast column to float
        normalized_data = normalized_data.withColumn(column, normalize(*interval)(sqlf.col(column))) #normalize column

    save_table(normalized_data, 'data/normalized_data.tsv')

if __name__ == '__main__':
    main()
