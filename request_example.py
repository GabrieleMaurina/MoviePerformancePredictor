from requests import get
from re import compile as cmp
from sys import argv

BASE = 'https://www.boxofficemojo.com'
SEARCH = 'https://www.boxofficemojo.com/search/?q='
LINK = cmp(r'<a class="a-size-medium a-link-normal a-text-bold" href="(\/title\/tt\S+)">')
BOX_OFFICE = cmp(r'<span class="money">\$(\S+)<\/span>\n')

def get_box_office(movie):

    #first request to BoxOfficeMojo to search the movie
    tokens = movie.lower().split()
    query = SEARCH + '+'.join(tokens)
    res = get(query).text
    
    #second request to BoxOfficeMojo to view data of specific movie
    link = BASE + LINK.search(res).group(1)
    res = get(link).text
    box_office = int(BOX_OFFICE.findall(res)[2].replace(',',''))
    return box_office

def main():
    movie = 'Batman Begins'
    if len(argv) > 1: movie = ' '.join(argv[1:])
    bo = get_box_office(movie)
    print(f'{movie} Box Office: ${bo}')

if __name__ == '__main__':
    main()