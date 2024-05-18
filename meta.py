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
URL = 'https://www.metacritic.com/browse/movie/?releaseYearMin=1910&releaseYearMax=2024&page='
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
meta_base_url = 'https://www.metacritic.com'
columns = ['title', 'metascore', 'summary', 'date', 'rating', 'rank', 'url']
movies = pd.DataFrame(columns=columns)
vpn_count = 0
# missed = [1,83,475,480,481,636,637,638,640,652,653,654,658,659,660,661,662,663,664,665,666,667,668,669,670,671,672,673]
missed = [484,499,650]
with open(os.path.join(path, 'movies_errors.json'), 'r') as f:
	errors = json.loads(f.read())

# for i in tqdm.tqdm(range(55,673)):
for i in tqdm.tqdm(missed):
	try:
		this_page = URL + str(i)
		headers = {'User-Agent': agents[random.randrange(0,len(agents)-1)]}
		res = requests.get(this_page, headers=headers)
		content = res.content.decode()
		soup = bs(content, 'lxml')
		movie_block = soup.findAll('div', type='movie')[0]
		atags = movie_block.findAll('a')
		for tag in atags:
			raw = tag.text.strip().split('\n')
			if 'Metascore' in tag.text:
				rank = raw[0].strip().split('.')[0]
				title = raw[0].strip()[len(str(rank))+2:]
				if len(raw) == 6:
					rating = raw[4].strip()[6:]
				else:
					rating = ""
				divs = tag.findAll('div')
				for div in divs:
					if div.attrs.get('class') is not None and "c-finderProductCard_description" in div.attrs.get('class'):
						summary = div.text.strip()
				url = meta_base_url + tag.get('href')
				spans = tag.findAll('span')
				metascore = ""
				date = ""
				for span in spans:
					if 'data-v-4cdca868' in span.attrs.keys():
						metascore = span.text.strip()
					if span.attrs.get('class') is not None and 'u-text-uppercase' in span.attrs.get('class'):
						date = span.text.strip()
			
				movies.loc[len(movies.index)] = [title, metascore, summary, date, rating, rank, url]
		movies.to_excel(os.path.join(path, 'movies.xlsx'), index=False)
		print('db size: ', len(movies.index)+24, 'rank count: ', rank)
		vpn_count += 1
		if vpn_count > 3:
			# time.sleep(120)
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



