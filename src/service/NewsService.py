from src.dao.NewsDAO import NewsDAO
from abc import ABC, abstractmethod
from typing import Literal, Any

class NewService(ABC):
    '''
    Service used to handle the business logics of News Summary
    '''
    @abstractmethod
    def __init__(self, news_dao: NewsDAO)->None:
        pass
    
    @abstractmethod
    def getListOfNews(self, list_of_tickers: list[str]|Literal['all']|None=None, limit:int|None=10)->list[dict[str, Any]]:  
        '''
        Returns a list of news in json object format
        '''    
        pass 

    @abstractmethod
    def getListOfUniqueCompanies(self)->list[str]:      
        '''
        Returns a list of unique companies
        '''    
        pass 