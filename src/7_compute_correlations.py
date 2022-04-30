from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import FloatType

def main():
    sc = SparkContext('local', '7_compute_correlations')
    sc.setLogLevel('ERROR') #hide useless logging
    spark = SparkSession(sc)

    normalized_data = spark.read.csv('data/normalized_data.tsv', sep=r'\t', header=True)
    normalized_data = normalized_data.drop('tconst', 'primaryTitle')
    
    def corr(col1, col2):
        global normalized_data
        normalized_data = normalized_data.withColumn(col1, sqlf.col(col1).cast(FloatType()))
        normalized_data = normalized_data.withColumn(col2, sqlf.col(col2).cast(FloatType()))
        return normalized_data.corr(col1, col2)

    cols = ('audience_score', 'averageRating', 'box_office', 'critics_score')
    correlations = [(col1, col2, corr(col1, col2)) for col1 in cols for col2 in cols if col2!=col1]

    cols1 = ('averageRating', 'box_office')
    cols2 = sorted(normalized_data.columns)
    correlations.extend((col1, col2, corr(col1, col2)) for col1 in cols1 for col2 in cols2 if col2!=col1)

    with open('data/correlations.tsv', 'w') as out:
        out.write('col1\tcol2\tcorr\n')
        for row in correlations:
            out.write('\t'.join(str(value) for value in row) + '\n')

if __name__ == '__main__':
    main()