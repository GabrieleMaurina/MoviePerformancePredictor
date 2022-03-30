from pyspark import SparkContext
from movie_requests import get_rt_data

INPUT = 'data/movies.tsv'
OUTPUT = 'data/rotton_tomatoes.tsv'

def main():
    sc = SparkContext('local', '2_collect_rotten_tomatoes_data')
    sc.setLogLevel('ERROR')
    sc.textFile(INPUT)

if __name__ == '__main__':
    main()