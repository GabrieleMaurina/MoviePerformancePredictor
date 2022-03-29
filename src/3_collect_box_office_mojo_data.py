from pyspark import SparkContext
from movie_requests import get_box_office

INPUT = 'data/movies.tsv'
OUTPUT = 'data/box_office_mojo.tsv'

def main():
    sc = SparkContext('local', '2_collect_box_office_mojo_data')
    sc.setLogLevel('ERROR')
    sc.textFile(INPUT)

if __name__ == '__main__':
    main()