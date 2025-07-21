from src.dao.NewsDAO import NewsDAO
from .NewsService import NewService
import numpy as np
from typing import Literal, Any, override

class NewServiceImpl(NewService):
    '''
    Implementation of NewService, used to handle the business logics of News Summary
    '''
    def __init__(self, news_dao: NewsDAO)->None:
        self.newsDAO = news_dao
    
    @override
    def getListOfNews(self, list_of_tickers: list[str]|Literal['all']|None=None, limit:int|None=10)->list[dict[str, Any]]:   
        '''
        This function is used to get list of news based on filter and reformat data retrieved from database

        Parameters
        ----------
        list_of_tickers: list[str], Literal['all'] or None, default None
            this can be list of strings which are tickers eg. ['TSLA', 'AAPL'] or the literal 'all' or None
        limit: int or None, default 10
            this is used to limit the number of records returned, it can be an integer or None

        Returns
        -------
        list[dict[str, Any]]
            a list of json object will be returned, which is a list of news objects
        '''    
        list_of_news_retrieved = self.newsDAO.getListOfNews(list_of_tickers=list_of_tickers, limit=limit)
        list_of_news_processed = []
        for news in list_of_news_retrieved:
            news_object = {"newsLink": news[0], "newsTitle": news[1], "newsDescription": news[2], "newsSource": news[3], "newsPublishTime": news[4], "tickers": np.array(news[5]).flatten().tolist()}
            if news[6]:
                news_object["newsSentiment"] = news[6]
            list_of_news_processed.append(news_object)
        return list_of_news_processed
    
    @override
    def getListOfUniqueCompanies(self)->list[str]:      
        '''
        This function is used to get list of unique companies and reformat data retrieved from database

        Returns
        -------
        list[str]
            a list of tickers eg. ['TSLA', 'NVDA'] will be returned, which is a list of strings
        '''    
        list_of_news_retrieved = np.array(self.newsDAO.getListOfUniqueCompanies()).flatten().tolist()
        return list_of_news_retrieved

