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
direct = 'https://www.rottentomatoes.com/m/'
search = 'https://www.rottentomatoes.com/search?search='
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
rotten_base_url = 'https://www.rottentomatoes.com/'
columns = ['title', 'genre', 'rating', 'summary', 'rotten', 'audience', 'release-date', 'known-year', 'url']
movies = pd.DataFrame(columns=columns)
vpn_count = 0

with open(os.path.join(path, 'movies_errors.json'), 'r') as f:
	errors = json.loads(f.read())
df = pd.read_excel(os.path.join(path, 'missing.xlsx'))
missing = dict(zip(df['title'], df['year']))
for k, v in missing.items():
	missing[k] = str(v).split('.')[0]

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
	
for title in tqdm.tqdm(missing.keys()):
	try:
		genre, rating, summary, rotten, audience, date, movie_url = '','','','','','',''
		title_split = '%20'.join(title.lower().split())
		search_page = search + title_split
		print('searching:', search_page)
		headers = {'User-Agent': agents[random.randrange(0,len(agents)-1)]}
		res = requests.get(search_page, headers=headers)
		print('status_code:', res.status_code)
		content = res.content.decode()
		soup = bs(content, 'lxml')
		search_results = soup.findAll('search-page-media-row')
		found_movie = False
		for r in search_results:
			year = r.get('releaseyear')
			if missing[title] == year:
				found_movie = True
				atag = r.find('a')
				movie_url = atag.get('href')
				print('found movie url:', movie_url)
				res = requests.get(movie_url, headers=headers)
				content = res.content.decode()
				soup = bs(content, 'lxml')
				rt_text = soup.findAll('rt-text')
				for rt in rt_text:
					if rt.get('slot') is not None:
						if 'ratingsCode' in rt.get('slot'):
							rating = rt.text
						if 'genre' in rt.get('slot'):
							genre = rt.text
				divs = soup.findAll('div')
				for div in divs:
					if div.get('class') is not None and 'synopsis-wrap' in div.get('class'):
						summary = div.text.replace('\nSynopsis\n','')
						break
				tags = soup.findAll('rt-button')
				for tag in tags:
					if tag.get('slot') is not None and 'criticsScore' in tag.get('slot'):
						rotten = tag.text.strip()
					if tag.get('slot') is not None and 'audienceScore' in tag.get('slot'):
						audience = tag.text.strip()
				sections = soup.findAll('section')
				for section in sections:
					if section.get('class') is not None and 'media-info' in section.get('class'):
						divs = section.findAll('div')
						for div in divs:
							if div.get('class') is not None and 'category-wrap' in div.get('class') and 'Release Date (Theaters)' in div.text:
								date = div.text.replace('\n',' ').strip()
							# if div.get('class') is not None and 'category-wrap' in div.get('class') and 'Release Date (Theaters)' in div.text:
				break
		if found_movie:
			movies.loc[len(movies.index)] = [title, genre, rating, summary, rotten, audience, date, missing[title], movie_url]
			print(title, genre, rating, rotten, audience, date, summary[0:30], movie_url)
			movies.to_excel(os.path.join(path, 'movies.xlsx'), index=False)
		time.sleep(2)
		
		vpn_count += 1
		if vpn_count > 10:
			time.sleep(10)
			vpn_count = 0
			rotate_VPN()
	
	except Exception as e:
		time.sleep(10)
		if len(movie_url) > 0:
			URL = movie_url
		else:
			URL = search_page
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