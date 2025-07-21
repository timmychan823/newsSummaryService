from .NewsDAO import NewsDAO
from typing import Literal, Any, override

class NewsDAOImpl(NewsDAO):
    '''
    Implementation of NewsDAO, used to retrieve data from database
    '''
    def __init__(self, conn)->None:
        self.conn = conn #TODO: how to check the if conn is an instance of connection?

    @override
    def getListOfNews(self, list_of_tickers:list[str]|Literal['all']|None=None, limit:int|None=10)->list[tuple[Any]]:
        '''
        This function is used to get list of news based on filter from the database

        Parameters
        ----------
        list_of_tickers: list[str], Literal['all'] or None, default None
            this can be list of strings which are tickers eg. ['TSLA', 'AAPL'] or the literal 'all' or None
        limit: int or None, default 10
            this is used to limit the number of records returned, it can be an integer or None

        Returns
        -------
        list[tuple[Any]]
            a list of tuple will be returned, which is a list of news objects from the database
        '''
        with self.conn.cursor() as curs: 
            query = """
                        SELECT link, "newsTitle", "newsDescription", "newsSource", "newsPublishTime", "tickers", "newsSentiment"
                        FROM public."NewsSummary"
                        WHERE
                    """
            if list_of_tickers=="all":
                query+=" 1=1"
            else:
                query+=""" "tickers" && ARRAY"""
                if list_of_tickers != None:
                    query+=str(list_of_tickers)
                else:
                    query+=str([])
                query+="::text[]"
            if limit==None:
                limit=10 
                query+=f"LIMIT {limit}"
            else:
                query+=f"LIMIT {limit}"
            query+=";"
            curs.execute(query)
            records = curs.fetchall()
            return records
    
    @override
    def getListOfUniqueCompanies(self)->list[tuple[str]]:
        '''
        This function is used to get list of unique companies from database

        Returns
        -------
        list[tuple[str]]
            a list of company tickers will be returned eg. [('TSLA',), ('NVDA',)], which is a list of company tickers, from the database
        '''
        with self.conn.cursor() as curs: 
            query = """
                        SELECT DISTINCT(unnest("tickers")) AS x
                        FROM public."NewsSummary"
                        ORDER BY x
                    """
            query+=";"
            curs.execute(query)
            records = curs.fetchall()
            return records