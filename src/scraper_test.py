from movie_requests import get_bom_data, get_rt_data
from os.path import isfile

INPUT = 'data/movies.tsv'
OUTPUT = 'data/scraped.tsv'

def main():
    if not isfile(OUTPUT):
        with open(OUTPUT, 'w') as out:
            out.write('tconst\tbox_office\tbudget\taudience_score\tcritics_score')
    with open(INPUT, 'r') as data:
        data = data.read()
    data = tuple(tuple(movie.split('\t')) for movie in data.split('\n') if movie)[1:]
    for movie in reversed(data):
        bo, bg = get_bom_data(movie[1] + ' ' + movie[3])
        us, cs = get_rt_data(movie[1] + ' ' + movie[3])
        with open(OUTPUT, 'a') as out:
            out.write('\t'.join((movie[0], bo, bg, us, cs)))

if __name__ == '__main__':
    main()