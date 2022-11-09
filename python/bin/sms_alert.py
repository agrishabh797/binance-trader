import logging
import sys
import time
from decimal import Decimal
from math import floor, ceil
from binance.lib.utils import config_logging
import psycopg2
from utils.read_env import read_env
from utils.db_utils import get_db_details
from datetime import datetime
import os
from binance.um_futures import UMFutures
import random
from twilio.rest import Client
from binance.error import ClientError

def send_sms(text_message, config):
    account_sid = config['TWILIO_ACCOUNT_SID']
    auth_token = config['TWILIO_AUTH_TOKEN']
    client = Client(account_sid, auth_token)

    message = client.messages \
        .create(
        body=text_message,
        from_='+18316041992',
        to='+917709452797'
    )

def main():
    # Set the config parameters using the config file
    current_time = datetime.utcnow()
    print(current_time)
    current_timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')

    home_dir = os.path.expanduser('~')
    workspace_dir = home_dir + '/binance-trader'
    config_file = workspace_dir + '/config/env_config.yaml'
    connections_file = home_dir + '/.secure/connections.yaml'
    configs = read_env(config_file)

    database_identifier = configs["DATABASE_IDENTIFIER"]

    conn_details = get_db_details(connections_file, database_identifier)
    conn = psycopg2.connect(database=conn_details["DATABASE_NAME"],
                            user=conn_details["USER"], password=conn_details["PASSWORD"],
                            host=conn_details["HOST_NAME"], port=conn_details["PORT"]
                            )

    text_position = ''

    query = """select symbol, net_pnl, created_ts from positions p where position_status = 'CLOSED';"""
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    print(rows)
    for row in rows:
        print(row)
        time_diff = (current_time - row[2]).total_seconds() / 3600
        if time_diff <= 1:
            text_position = text_position + str(row[0]) + " closed with NET PNL " + str(round(float(row[1]), 2)) + "\n"

    if text_position:
        text_position = "Since Last hour - \n" + text_position
        twilio_keys = get_db_details(connections_file, 'TWILIO_KEY')
        send_sms(text_position, twilio_keys)