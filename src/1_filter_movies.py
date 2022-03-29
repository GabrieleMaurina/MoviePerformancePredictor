from pyspark import SparkContext

INPUT = 'data/title.basics.tsv'
OUTPUT = 'data/movies.tsv'

def main():
    sc = SparkContext('local', '1_filter_movies')
    sc.setLogLevel('ERROR')
    sc.textFile(INPUT)

if __name__ == '__main__':
    main()