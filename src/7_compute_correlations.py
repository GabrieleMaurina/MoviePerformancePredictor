from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from json import dump

def main():
    sc = SparkContext('local', '7_compute_correlations')
    sc.setLogLevel('ERROR') #hide useless logging
    spark = SparkSession(sc)

    normalized_data = spark.read.csv('data/normalized_data.tsv', sep=r'\t', header=True)
    normalized_data = normalized_data.drop('tconst', 'primaryTitle')
    
    cols = sorted(normalized_data.columns)
    correlations = {col1:{col2: normalized_data.corr(col1, col2) for col2 in cols} for col1 in cols}
    with open('data/correlations.json', 'w') as out:
        dump(correlations, out, indent=4)

if __name__ == '__main__':
    main()