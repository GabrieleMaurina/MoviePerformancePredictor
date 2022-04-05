from movie_requests import get_bom_data, get_rt_data
from os.path import isfile

INPUT = 'data/movies.tsv'
OUTPUT = 'data/scraped.tsv'

def main():
    if isfile(OUTPUT):
        with open(OUTPUT, 'r') as out:
            start = tuple(line for line in out.read().split('\n') if line)[-1].split('\t')[0]
        if start == 'tconst': start = None
    else:
        with open(OUTPUT, 'w') as out:
            out.write('tconst\tbox_office\tbudget\taudience_score\tcritics_score\n')
        start = None
    with open(INPUT, 'r') as data:
        data = data.read()
    data = tuple(tuple(movie.split('\t')) for movie in data.split('\n') if movie)[:0:-1]
    if start:
        for i, movie in enumerate(data):
            if movie[0] == start:
                start = i
                break
    for movie in data[start if start else 0:]:
        bo, bg = get_bom_data(movie[1] + ' ' + movie[3])
        if bo is None or bg is None:
            bo, bg = get_bom_data(movie[1])
        us, cs = get_rt_data(movie[1] + ' ' + movie[3])
        if us is None or cs is None:
            us, cs = get_rt_data(movie[1])
        print(movie[0], movie[1], movie[3], bo, bg, us, cs)
        with open(OUTPUT, 'a') as out:
            out.write('\t'.join((movie[0], str(bo), str(bg), str(us), str(cs))) + '\n')

if __name__ == '__main__':
    main()
