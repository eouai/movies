import os
import json
import time
import tqdm
import random
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from nordvpn_switcher import initialize_VPN, rotate_VPN, terminate_VPN

initialize_VPN(save=1, area_input=['United States'])
URL = 'https://www.imdb.com/find/?q='
agents = ['Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36',
			'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36',
			'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36',
			'Mozilla/5.0 (CrKey armv7l 1.5.16041) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.0 Safari/537.36',
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.2478.80',
			'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.2478.80',
			'Mozilla/5.0 (Linux; Android 10; HD1913) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.113 Mobile Safari/537.36 EdgA/124.0.2478.62',
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
			'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',]
			

path = os.path.normpath('C:\\Users\\Burt\\Documents\\Git\\podcast\\')
imdb_base_url = 'https://imdb.com'
columns = ['searched_title', 'scraped_title', 'imdb_score', 'meta_score', 'summary', 'date', 'rating', 'genres', 'url', 'writers', 'stars', 'directors', 'volume']
movies = pd.DataFrame(columns=columns)
vpn_count = 0

with open(os.path.join(path, 'movies_errors.json'), 'r') as f:
	errors = json.loads(f.read())
df = pd.read_excel(os.path.join(path, 'missing.xlsx'))
missing = dict(zip(df['url'], df['title']))

def parse_json_recursively(json_object, target_key, search_res=[]):
	if type(json_object) is dict and json_object:
		for key in json_object:
			if key == target_key:
				search_res.append(json_object[key])
			parse_json_recursively(json_object[key], target_key, search_res)
	elif type(json_object) is list and json_object:
		for item in json_object:
			parse_json_recursively(item, target_key, search_res)
	return search_res

for url, title in tqdm.tqdm(missing.items()):
	try:
		scraped_title, imdb_score, meta_score, summary, date, rating, genres, writers, stars, directors, volume = '','','','','','',[],[],[],[],''
		time.sleep(3)
		headers = {'User-Agent': agents[random.randrange(0,len(agents)-1)]}
		movie_res = requests.get(url, headers=headers)
		content = movie_res.content.decode()
		soup = bs(content, 'lxml')
		spans = soup.findAll('span')
		for span in spans:
			if span.get('class') is not None and 'three-Elements' in span.get('class'):
				if 'Metascore' in span.text:
					meta_score = span.text.replace('Metascore','')
			if span.get('class') is not None and 'hero__primary-text' in span.get('class'):
				scraped_title = span.text
			if span.attrs.get('data-testid') is not None and 'plot-xl' in span.attrs.get('data-testid'):
				summary = span.text
		divs = soup.findAll('div')
		for div in divs:
			if div.attrs.get('data-testid') is not None and 'hero-rating-bar__aggregate-rating__score' in div.attrs.get('data-testid'):
				imdb_score = div.text
			if div.attrs.get('class') is not None and 'sc-bde20123-3' in div.attrs.get('class'):
				volume = div.text
		atags = soup.findAll('a')
		for tag in atags:
			if 'parentalguide' in tag.get('href'):
				rating = tag.text
			if 'releaseinfo?ref_=tt_dt_rdat' in tag.get('href'):
				if tag.attrs.get('aria-label') is None:
					date = tag.text
			if 'tt_ov_dr' in tag.get('href') and len(tag.text)>2:
				directors.append(tag.text)
			if 'tt_ov_wr' in tag.get('href') and len(tag.text)>2 and tag.text != 'Writers':
				writers.append(tag.text)
			if 'tt_ov_st' in tag.get('href') and len(tag.text)>2 and tag.text != 'Stars':
				stars.append(tag.text)
		stars = ', '.join(star for star in set(stars))
		directors = ', '.join(director for director in set(directors))
		writers = ', '.join(writer for writer in set(writers))
			
		scripts = soup.findAll('script')
		for script in scripts:
			if script.get('id') == '__NEXT_DATA__':
				data = json.loads(script.text)
				break
		raw_genres = parse_json_recursively(data, 'genres', [])[0].get('genres')
		for genre in raw_genres:
			genres.append(genre.get('text'))
		for genre in genres:
			movies.loc[len(movies.index)] = [title, scraped_title, imdb_score, meta_score, summary, date, rating, genre, url, writers, stars, directors, volume]
			print('searched title:', title, 'scraped title:', scraped_title, imdb_score, meta_score, summary[0:20], date, rating, genre, url, writers, stars, directors, volume)
		movies.to_excel(os.path.join(path, 'movies.xlsx'), index=False)

		vpn_count += 1
		if vpn_count > 10:
			time.sleep(10)
			vpn_count = 0
			rotate_VPN()

	except Exception as e:
		time.sleep(10)
		if errors.get(URL) is None:
			errors[URL] = {'count': 1, str(e): 1}
		else:
			errors[URL]['count'] += 1
			if errors.get(URL).get(str(e)) is None:
				errors[URL][str(e)] = 1
			else:
				errors[URL][str(e)] += 1
		with open(os.path.join(path, 'movies_errors.json'), 'w') as f:
			json.dump(errors, f)
		print(str(e))
