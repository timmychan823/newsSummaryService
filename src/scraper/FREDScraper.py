import requests
import asyncio
from datetime import datetime
import pandas as pd
import itertools
from src.constant.API_constants import *

class FredScraper:
    def __init__(self, api_key, session=requests.Session()):
        self.api_key = api_key
        self.session = session
    async def get_release_sort_by_date(self):
        params = {'api_key': self.api_key, 'file_type': 'json'}
        res = await asyncio.to_thread(self.session.get, f"{FRED_API_BASE_URL}/releases/dates", params=params, verify=False)
        res.raise_for_status()
        data = await asyncio.to_thread(res.json)
        list_of_release_dates = []
        format_of_date = '%Y-%m-%d'
        for date in data['release_dates']:
            release_dates = {'release_id': date['release_id'], 'release_name': date['release_name'], 'release_date': datetime.strptime(date['date'], format_of_date)}
            list_of_release_dates.append(release_dates)
        return list_of_release_dates 
    async def get_releases_of_source(self, source_id):
        params = {'api_key': self.api_key, 'source_id': source_id, 'file_type': 'json'}
        res = await asyncio.to_thread(self.session.get, f"{FRED_API_BASE_URL}/source/releases", params=params, verify=False)
        res.raise_for_status()
        data = await asyncio.to_thread(res.json)
        list_of_release_dates = []
        for release in data['releases']:
            release_object= {'release_id': release.get('id'), 'release_name': release.get('name'), 'release_link': release.get('link'), 'source_id': source_id}
            list_of_release_dates.append(release_object)
        return list_of_release_dates
    async def get_sources(self):
        params = {'api_key': self.api_key, 'file_type': 'json'}
        res = await asyncio.to_thread(self.session.get, f"{FRED_API_BASE_URL}/sources", params=params, verify=False)
        res.raise_for_status()
        data = await asyncio.to_thread(res.json)
        list_of_sources = []
        for source in data['sources']:
            source_object= {'source_id': source.get('id'), 'source_name': source.get('name'), 'source_link': source.get('link')}
            list_of_sources.append(source_object)
        return list_of_sources
    def __del__(self):
        self.session.close()