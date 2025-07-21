import psycopg2
from src.constant.Db_constants import * 
from src.dao.NewsDAOImpl import NewsDAOImpl
from src.service.NewsServiceImpl import NewServiceImpl
from src.service.DataReleaseImpl import DataReleaseServiceImpl
from flask import Flask, request, render_template
from flask_cors import CORS
import requests
import logging
import json

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') #change this to DEBUG mode for debugging
app = Flask(__name__)
cors = CORS(app)

pg_connection_dict = {
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PW,
    'port': DB_PORT,
    'host': DB_HOST
}
con = psycopg2.connect(**pg_connection_dict)
newsDAO = NewsDAOImpl(con)
newServiceImpl = NewServiceImpl(newsDAO)
dataReleaseServiceImpl = DataReleaseServiceImpl()

@app.route('/listOfNews', methods = ['GET'])
def list_of_news():
    if request.method == 'GET':
        if request.args.get('tickers')=='all':
            tickers='all'
        else:
            tickers = request.args.getlist('tickers')
        limit = int(request.args.get('limit')) #TODO: should also get page number of desired page, startTime, endTime
        logger.info(f"tickers: {tickers}, limit: {limit}")
        list_of_news = (newServiceImpl.getListOfNews(tickers, limit))
        return list_of_news #TODO: should also count the total number of news and return page number of desired page 

@app.route('/listOfUniqueCompanies', methods = ['GET'])
def list_of_unique_companies():
    if request.method == "GET":
        list_of_unique_companies = (newServiceImpl.getListOfUniqueCompanies())
        return list_of_unique_companies

@app.route('/dataReleases', methods = ['GET'])
async def list_of_releases():
    if request.method == 'GET':
        out = await dataReleaseServiceImpl.getDataRelease()
        return out

if __name__ == '__main__':
    app.run(debug = True, host = '0.0.0.0', port=15000)




