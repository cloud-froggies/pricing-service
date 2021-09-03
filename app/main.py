from fastapi import FastAPI, Query, HTTPException
from fastapi.logger import logger
from typing import List, Optional
import random
import pymysql
import os
import logging 

DB_ENDPOINT = os.environ.get('db_endpoint')
DB_ADMIN_USER = os.environ.get('db_admin_user')
DB_ADMIN_PASSWORD = os.environ.get('db_admin_password')
DB_NAME = os.environ.get('db_name')


app = FastAPI(title='Pricing Service',version='0.1')
gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)

if __name__ != "main":
    logger.setLevel(gunicorn_logger.level)
else:
    logger.setLevel(logging.DEBUG)


def get_db_conn():
    try:
        conn = pymysql.connect(host=DB_ENDPOINT, user=DB_ADMIN_USER, passwd=DB_ADMIN_PASSWORD, db=DB_NAME, connect_timeout=5)
        return conn
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        raise


@app.get("/pricing")
def pricing(advertiser_campaigns: str, advertiser_campaigns_bids: str, publisher: int):
    # Split multiple values
    campaigns = [int(i) for i in advertiser_campaigns.split(",")]
    bids = [float(i) for i in advertiser_campaigns_bids.split(",")]

    conn = get_db_conn()
    logger.error(advertiser_campaigns, advertiser_campaigns_bids, publisher)

    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        query = """SELECT commission
        FROM publishers
        WHERE id = %s"""
        cursor.execute(query,(publisher))

    result = cursor.fetchone()
    if result:
        commision = result['commission']
        prices = [x * commision for x in bids]
        response = []
        for index in range(0, len(campaigns)):
            response.append({'id':campaigns[index], 'price':prices[index]})
        return response
    else:
        raise HTTPException(status_code=404, detail= f'No se encuentra al publisher con id {publisher}.')
        



