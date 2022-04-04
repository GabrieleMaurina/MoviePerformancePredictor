#!pip3 install rotten_tomatoes_scraper
import rotten_tomatoes_scraper as rt

from rotten_tomatoes_scraper.rt_scraper import MovieScraper
movie_scraper = MovieScraper(movie_title='The Batman')
movie_scraper.extract_metadata()
print(movie_scraper.metadata)
