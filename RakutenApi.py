import json
from pathlib import Path
from itertools import chain

import requests
from urllib.parse import urlencode
from tqdm import tqdm

import time
import pymongo

def flatten(arrays):
	return list(chain.from_iterable(arrays))

def dump_json(file_name, object):
	Path('.').joinpath(file_name).write_text(json.dumps(object, indent=4, ensure_ascii=False), encoding='utf-8')

def sleep(min):
	time.sleep(min)

class RakutenClient:

	__endpoint = 'https://app.rakuten.co.jp/services/api'
	__format_version = '2'

	def __init__(self, application_id, path, version, endpoint=__endpoint, format_version=__format_version):
		self.application_id = application_id
		self.path = path
		self.version = version
		self.data = {}
		self.__endpoint = endpoint
		self.__format_version = format_version

	@property
	def request_path(self):
		return '{endpoint}/{path}/{version}'.format(endpoint=self.__endpoint, path=self.path, version=self.version)

	def _req(self, **kwargs):
		params = {'applicationId': self.application_id, 'formatVersion': self.__format_version, **kwargs}
		query_string = urlencode(params)
		url = '{request_path}?{query_string}'.format(request_path=self.request_path, query_string=query_string)
		res = requests.get(url)
		data = res.json()
		if 'error' in data:
			raise Exception(data)
		return data

	def get(self, **kwargs):
		data = self._req(**kwargs)
		self.data = data
		return self

	def json(self):
		return self.data

	def dump(self, file_name):
		dump_json(file_name, self.data)
		return self

class Genre(RakutenClient):

	def __init__(self, application_id, path='IchibaGenre/Search', version='20140222'):
		super().__init__(application_id, path, version)
		self.data = {}

	def get_one(self, genre_id='0', genre_path='0'):
		return self._req(genreId=genre_id, genrePath=genre_path)

	def get(self, genre_id='0', max_depth=2):
		# すでに保存されたファイルがあればAPIへリクエストを行わずそれを読み込む
		file_path = Path('.').joinpath('genre_tree_{genre_id}_{max_depth}.json'.format(genre_id=genre_id, max_depth=max_depth))
		if file_path.exists():
			self.data = json.loads(file_path.read_text(encoding='utf-8'))
			return self
		# APIへリクエスト
		res = self.get_one(genre_id=genre_id)
		data = res['current']
		# childrenがあれば再帰的にリクエスト
		if(data['genreLevel'] < max_depth):
			data['children'] = [self.get(genre_id=child['genreId'], max_depth=max_depth).json() for child in tqdm(res['children'])]
		self.data = data
		return self

	def save(self):
		client = pymongo.MongoClient("localhost", 27017)
		db = client.rakuten_api
		genre_list = self.json(flatten=True)
		for genre_item in genre_list:
			genre_id = genre_item['genreId']
			genre_name = genre_item['genreName']
			item = {'genreId': genre_id, 'genreName': genre_name}
			# DBに保存
			db.genre.update({'genreId': genre_id}, item, True)
		return self

	def __flatten_tree(self, tree):
		item_without_children = {key:tree[key] for key in tree.keys() if key !='children'}
		items = [item_without_children]
		if 'children' in tree:
			items.extend(flatten([self.__flatten_tree(child) for child in tree['children']]))
		return items

	def json(self, flatten=False):
		if not flatten:
			return super().json()
		else:
			return self.__flatten_tree(self.data)

	def dump(self, file_name, flatten=False):
		if not flatten:
			return super().dump(file_name)
		else:
			dump_json(file_name, self.json(flatten=True))
			return self

class Rank(RakutenClient):

	def __init__(self, application_id, path='IchibaItem/Ranking', version='20170628'):
		super().__init__(application_id, path, version)
		self.data = []

	def get(self, genre_id='0', page=1):
		self.genre_id = genre_id
		rank_list = self._req(genreId=genre_id, page=page)['Items']
		self.data = [{'genreId': genre_id, 'itemCode': item['itemCode'], 'itemName': item['itemName'], 'rank': item['rank']} for item in rank_list]
		return self

	def save(self, top_n=10):
		client = pymongo.MongoClient("localhost", 27017)
		db = client.rakuten_api
		genre_id = self.genre_id
		items = self.data[0:top_n]
		for item in items:
			item_code = item['itemCode']
			# DBに保存
			db.rank.update({'itemCode': item_code}, item, True)
		return self

if __name__ == '__main__':

	application_id = 'your_rakuten_application_id'

	# application_idを指定してGenreインスタンスを作成
	genre = Genre(application_id)

	# ツリーを構築
	# max_depth=0で1階層目まで、max_depth=1で2階層目まで、max_depth=2で3階層目まで。
	# Genreはgetするとself.dataに構築したツリーが保持される
	genre.get(genre_id=0, max_depth=2)

	# 構築したツリーをjsonテキストとして保存
	genre.dump('genre_tree_0_2.json')

	# DBに保存
	genre.save()

	# ツリーをフラットにした配列を取得
	genre_list = genre.json(flatten=True)

	# Rankインスタンスを作成
	rank = Rank(application_id)

	# ランクAPIテスト
	rank.get(genre_id=0).dump('rank_0.json')

	# 取得したツリーからgenreIdをインプットとしてrankを取得
	for genre_item in genre_list:
		if genre_item["genreLevel"] ==1:
			genre_id = genre_item["genreId"]
			try:
				rank.get(genre_id, page=1).save(top_n=10).dump('rank_{}.json'.format(genre_id))
				sleep(1)
			except:
				print(genre_item)
