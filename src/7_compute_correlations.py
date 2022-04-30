from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from json import dump

def main():
    sc = SparkContext('local', '7_compute_correlations')
    sc.setLogLevel('ERROR') #hide useless logging
    spark = SparkSession(sc)

    normalized_data = spark.read.csv('data/normalized_data.tsv', sep=r'\t', header=True)
    normalized_data = normalized_data.drop('tconst', 'primaryTitle')
    
    cols = ('audience_score', 'averageRating', 'box_office', 'critics_score')
    correlations = [(col1, col2, normalized_data.corr(col1, col2)) for col1 in cols for col2 in cols if col2!=col1]
    
    cols1 = ('averageRating', 'box_office')
    cols2 = sorted(normalized_data.columns)
    correlations.extend((col1, col2, normalized_data.corr(col1, col2)) for col1 in cols1 for col2 in cols2 if col2!=col1)
    
    with open('data/correlations.json', 'w') as out:
        dump(correlations, out, indent=4)

if __name__ == '__main__':
    main()