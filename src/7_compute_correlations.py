from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import FloatType
from itertools import chain, combinations, product
from math import isnan

def cap(v):
    return 0.0 if isnan(v) or v < 0.0 else v

def main():
    sc = SparkContext('local', '7_compute_correlations')
    sc.setLogLevel('ERROR') #hide useless logging
    spark = SparkSession(sc)

    normalized_data = spark.read.csv('data/normalized_data.tsv', sep=r'\t', header=True)
    normalized_data = normalized_data.drop('tconst', 'primaryTitle')

    cols1 = ('audience_score', 'averageRating', 'box_office', 'critics_score')
    cols2 = sorted(normalized_data.columns)
    cols = chain(combinations(cols1, 2), product(cols1, cols2))

    for col in cols2:
        normalized_data = normalized_data.withColumn(col, sqlf.col(col).cast(FloatType()))

    correlations = [(col1, col2, cap(normalized_data.corr(col1, col2))) for col1, col2 in cols if col1!=col2]

    with open('data/correlations.tsv', 'w') as out:
        out.write('col1\tcol2\tcorr\n')
        for row in correlations:
            out.write('\t'.join(str(value) for value in row) + '\n')

if __name__ == '__main__':
    main()
