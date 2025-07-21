from abc import ABC, abstractmethod
from typing import Literal, Any

class NewsDAO(ABC):
    '''
    DAO used to retrieve data from database
    '''
    @abstractmethod
    def __init__(self, conn)->None:
        pass
    
    @abstractmethod
    def getListOfNews(self, list_of_tickers:list[str]|Literal['all']|None=None, limit:int|None=10)->list[tuple[Any]]:
        '''
        Returns a list of filtered news from database
        '''
        pass

    @abstractmethod
    def getListOfUniqueCompanies(self)->list[tuple[str]]:
        '''
        Returns a list of unique companies from database
        '''
        pass