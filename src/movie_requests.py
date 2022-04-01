from requests import get, post
from re import compile as cmp
from json import dumps, loads
from sys import argv

TIMEOUT = 3

BOM_BASE = 'https://www.boxofficemojo.com'
BOM_SEARCH = 'https://www.boxofficemojo.com/search/?q='
BOM_LINK = cmp(r'<a class="a-size-medium a-link-normal a-text-bold" href="(\/title\/tt\S+)">')
BOM_MONEY = cmp(r'<span class="money">\$(\S+?)<\/span>')

RT_SEARCH = 'https://79frdp12pn-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.13.0)%3B%20Browser%20(lite)&x-algolia-api-key=175588f6e5f8319b27702e4cc4013561&x-algolia-application-id=79FRDP12PN'
RT_PAYLOAD = {'requests': ({'indexName':'content_rt', 'params': 'filters=rtId%20%3E%200%20AND%20isEmsSearchable%20%3D%201&hitsPerPage=5&analyticsTags=%5B%22header_search%22%5D&clickAnalytics=true'},{'indexName':'people_rt', 'params': 'filters=rtId%20%3E%200%20AND%20isEmsSearchable%20%3D%201&hitsPerPage=5&analyticsTags=%5B%22header_search%22%5D&clickAnalytics=true'})}

def get_bom_data(movie):
    try:
        #first request to BoxOfficeMojo to search the movie
        tokens = movie.lower().split()
        query = BOM_SEARCH + '+'.join(tokens)
        res = get(query, timeout=TIMEOUT).text
        
        #second request to BoxOfficeMojo to view data of specific movie
        link = BOM_BASE + BOM_LINK.search(res).group(1)
        res = get(link, timeout=TIMEOUT).text
        money = BOM_MONEY.findall(res)
        box_office = int(money[2].replace(',',''))
        budget = int(money[4].replace(',',''))
        return box_office, budget
    except Exception:
        return (None,None)

def get_rt_data(movie):
    try:
        payload = dict(RT_PAYLOAD)
        payload['requests'][0]['query'] = movie
        payload['requests'][1]['query'] = movie
        res = post(RT_SEARCH, data=dumps(payload), timeout=TIMEOUT).json()
        scores = res['results'][0]['hits'][0]['rottenTomatoes']
        return (scores['audienceScore'], scores['criticsScore'])
    except Exception:
        return (None, None)

def main():
    movie = 'Batman Begins'
    if len(argv) > 1: movie = ' '.join(argv[1:])
    bo, bg = get_bom_data(movie)
    us, cs = get_rt_data(movie)
    print(movie)
    print('Box Office:', bo)
    print('Budget:', bg)
    print('User score:', us)
    print('Critics score:', cs)

if __name__ == '__main__':
    main()
