from difflib import SequenceMatcher
from nltk.corpus import stopwords

INPUT = 'data/movie_franchise.csv'
OUTPUT = 'data/franchises.tsv'

STOP_WORDS = set(stopwords.words('english'))

def preprocess_title(title):
    return ' '.join(filter(lambda w: w not in STOP_WORDS, title.lower().split()))

def franchise(title1, title2):
    matcher = SequenceMatcher(None, title1, title2)
    ratio = matcher.ratio()
    match_size = matcher.find_longest_match(0,len(title1),0,len(title2)).size
    return ratio > 0.9 or (ratio > 0.7 and match_size > 7) or match_size > 12

def dfs(movies, movie, f):
    movie[2] = False
    movie[4] = f
    for neighbor in movie[3]:
        neighbor = movies[neighbor]
        if neighbor[2]:
            dfs(movies, neighbor, f)

def main():
    with open(INPUT, 'r') as movies:
        movies = tuple(movie.split(',') for movie in movies.read().split('\n') if movie and not movie.startswith('tconst'))
    movies = {movie[1]: [movie[5], preprocess_title(movie[5]), True, [], None] for movie in movies}
    edges = 0
    tot = len(movies)
    movies_tuple = tuple(movies.items())
    for i in range(tot):
        id1, (title1, pptitle1, _, neighbors1, _) = movies_tuple[i]
        for j in range(i+1, tot):
            id2, (title2, pptitle2, _, neighbors2, _) = movies_tuple[j]
            if franchise(pptitle1, pptitle2):
                neighbors1.append(id2)
                neighbors2.append(id1)
                edges += 1
    for movie in tuple(movies.keys()):
        if not movies[movie][3]:
            del movies[movie]
    f = 1
    for movie in movies.values():
        if movie[2]:
            dfs(movies, movie, f)
            f += 1
    movies = tuple((movie, movies[movie][4]) for movie in movies)
    with open(OUTPUT, 'w') as out:
        out.write('tconst\tfranchise\n')
        out.write('\n'.join(f'{tconst}\t{franchise}' for tconst, franchise in movies))

if __name__ == '__main__':
    main()
