from pyspark import SparkContext
from os.path import join

BASE = 'file:///s/chopin/a/grad/gmaurina/workspace/cs535/team_project'
INPUT = 'data/title.basics.tsv'
OUTPUT = 'data/movies.tsv'

def main():
    sc = SparkContext('local', '1_filter_movies')
    sc.setLogLevel('ERROR')
    movies = sc.textFile(join(BASE, INPUT))
    movies.take(10).foreach(println)

if __name__ == '__main__':
    main()
