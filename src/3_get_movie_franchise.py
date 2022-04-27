from difflib import SequenceMatcher
from nltk.corpus import stopwords

INPUT = 'data/movies.tsv'
OUTPUT = 'data/franchises.tsv'

STOP_WORDS = set(stopwords.words('english'))

def preprocess_title(title):
    return ' '.join(filter(lambda w: w not in STOP_WORDS, title.lower().split()))

def franchise(title1, title2):
    matcher = SequenceMatcher(None, title1, title2)
    ratio = matcher.ratio()
    match_size = matcher.find_longest_match(0,len(title1),0,len(title2)).size
    return ratio > 0.9 or (ratio > 0.7 and match_size > 7) or match_size > 12

def dfs(movies, movie):
    movie[1] = False
    for neighbor in movies[id][2]:
        neighbor = movies[neighbor]
        if neighbor[1]:
            dfs(movies, neighbor)

def main():
    with open(INPUT, 'r') as movies:
        movies = tuple(movie.split('\t')[:2] for movie in movies.read().split('\n') if movie and not movie.startswith('tconst'))
    movies = {movie[0]: [movie[1], preprocess_title(movie[1]), True, []] for movie in movies}
    i = 0
    for id1, (title1, _, neighbors1) in movies.items():
        for id2, (title2, _, neighbors2) in movies.items():
            if id1 != id2 and id1 not in neighbors2 and franchise(title1, title2):
                neighbors1.append(id2)
                neighbors2.append(id1)
                i += 1
                print(title1, title2)
    print(i, len(movies))
    #for movie in movies.values:
    #    if movie[1]:
    #        dfs(movies, movie)

if __name__ == '__main__':
    main()
