from pyspark import SparkContext

OUTPUT = 'data/all_data.tsv'

def main():
    sc = SparkContext('local', '4_join_all_data')
    sc.setLogLevel('ERROR')

if __name__ == '__main__':
    main()