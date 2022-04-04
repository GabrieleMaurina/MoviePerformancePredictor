from pyspark import SparkContext

OUTPUT = 'data/data.tsv'

def tsv(v):
    return v.split('\t')

def tconst(v):
    return v[0] != 'tconst'

def nconst(v):
    return v[0] != 'nconst'

def titleId(v):
    return v[0] != 'titleId'

def main():
    sc = SparkContext('local', '4_join_data')
    sc.setLogLevel('ERROR')

    movies = sc.textFile('/content/data/movies.tsv').map(tsv).filter(tconst).map(lambda v: v[0])
    basics = sc.textFile('/content/data/title.basics.tsv').map(tsv).filter(tconst).map(lambda v: (v[0], (v[2], v[5], v[7], v[8])))
    akas = sc.textFile('/content/data/title.akas.tsv').map(tsv).filter(titleId)
    crew = sc.textFile('/content/data/title.crew.tsv').map(tsv).filter(tconst).map(lambda v: (v[0], (v[1], v[2])))
    principals = sc.textFile('/content/data/title.principals.tsv').map(tsv).filter(tconst)
    rating = sc.textFile('/content/data/title.rating.tsv').map(tsv).filter(tconst)
    name = sc.textFile('/content/data/name.basics.tsv').map(tsv).filter(nconst)

    basics.join(crew).take(10)
    basics.join(movies).take(10)

if __name__ == '__main__':
    main()