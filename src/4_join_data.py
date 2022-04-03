from pyspark import SparkContext

OUTPUT = 'data/data.tsv'

def tsv(v):
    return v.split('\t')

def main():
    sc = SparkContext('local', '4_join_data')
    sc.setLogLevel('ERROR')
    
    movies = sc.textFile('/content/data/movies.tsv').map(tsv)
    basics = sc.textFile('/content/data/title.basics.tsv').map(tsv)
    akas = sc.textFile('/content/data/title.akas.tsv').map(tsv)
    crew = sc.textFile('/content/data/title.crew.tsv').map(tsv)
    principals = sc.textFile('/content/data/title.principals.tsv').map(tsv)
    rating = sc.textFile('/content/data/title.rating.tsv').map(tsv)
    name = sc.textFile('/content/data/name.basics.tsv').map(tsv)

if __name__ == '__main__':
    main()