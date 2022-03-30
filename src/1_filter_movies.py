from pyspark import SparkContext

INPUT = 'data/title.basics.tsv'
OUTPUT = 'data/movies.tsv'

def main():
    sc = SparkContext('local', '1_filter_movies')
    sc.setLogLevel('ERROR')
    movies = sc.textFile(INPUT)
    movies = movies.map(lambda v: v.split('\t')).filter(lambda v: v[1]=='movie' and v[4]=='0').map(lambda v: '\t'.join((v[0],v[2],v[3],v[5]))).collect()
    with open(OUTPUT, 'w', encoding='utf-8') as out:
        out.write('tconst\tprimaryTitle\toriginalTitle\tstartYear\n')
        out.write('\n'.join(movies))

if __name__ == '__main__':
    main()
