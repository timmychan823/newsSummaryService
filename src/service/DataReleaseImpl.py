import requests
import asyncio
from datetime import datetime
import pandas as pd
import itertools
import json
from src.scraper.FREDScraper import FredScraper
from src.constant.API_constants import *

class DataReleaseServiceImpl:
    def __filter_release(self, list_of_releases_dates, list_of_releases_of_permitted_sources, list_of_sources, startTime, endTime):
        df = pd.DataFrame(list_of_releases_dates)
        df2 = pd.DataFrame(list_of_releases_of_permitted_sources)
        df3 = pd.DataFrame(list_of_sources)
        new_df = df.merge(df2, on='release_id')
        filter_df = new_df[(new_df['release_date']>=startTime) & (new_df['release_date']<endTime)]
        out_df = filter_df.merge(df3, on='source_id')
        return out_df

    async def getDataRelease(self):
        scraper = FredScraper(FRED_API_KEY)
        permitted_source_id = [1] #TODO: get it from DB
        list_of_releases_sort_by_date = await scraper.get_release_sort_by_date()
        get_releases_of_source_tasks = [scraper.get_releases_of_source(source_id) for source_id in permitted_source_id]
        list_of_releases_of_source_tasks = await asyncio.gather(*get_releases_of_source_tasks)
        list_of_releases_of_source_tasks = list(itertools.chain.from_iterable(list_of_releases_of_source_tasks))
        list_of_sources = await scraper.get_sources()
        result = (self.__filter_release(list_of_releases_sort_by_date, list_of_releases_of_source_tasks, list_of_sources, '2025-07-01', '2025-07-19'))
        result_by_date = result.groupby(['release_date', 'source_name'])
        
        out = dict()
        for group_name, group_df in result_by_date:
            group_date = datetime.strftime(group_name[0], '%Y-%m-%d')
            out.setdefault(group_date,[])
            out[group_date].append({'source_name': str(group_name[1]), 'releases': json.loads(group_df.to_json(orient = "records"))})
        return out