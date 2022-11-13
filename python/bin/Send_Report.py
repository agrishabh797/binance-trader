import logging
import sys
import time
from decimal import Decimal
from math import floor, ceil
from binance.lib.utils import config_logging
import psycopg2
from utils.read_env import read_env
from utils.db_utils import get_db_details
from datetime import datetime, timedelta
from datetime import date
import os
from binance.um_futures import UMFutures
import random
from twilio.rest import Client
import plivo
from binance.error import ClientError
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_sms(text_message, config, sms_app):

    account_sid = config['ACCOUNT_SID']
    auth_token = config['AUTH_TOKEN']
    if sms_app == 'TWILIO':
        client = Client(account_sid, auth_token)
        message = client.messages \
            .create(
            body=text_message,
            from_='+18316041992',
            to='+917709452797'
        )
    elif sms_app == 'PLIVO':
        client = plivo.RestClient(account_sid, auth_token)
        message = client.messages.create(
            src='RISHABH',
            dst='+917709452797',
            text=text_message, )
    print(message)


def get_utilized_wallet_amount(conn):
    sql = "select coalesce(sum(current_margin), 0) from positions where position_status = 'OPEN'"

    cursor = conn.cursor()
    cursor.execute(sql)

    utilized_wallet_amount = cursor.fetchone()[0]

    return utilized_wallet_amount

def get_unused_wallet_amount(um_futures_client):
    account_info = um_futures_client.account()
    unused_amount = float(account_info['totalOpenOrderInitialMargin']) + float(account_info['maxWithdrawAmount'])
    return unused_amount


def get_total_wallet_amount(conn, um_futures_client):
    total_wallet_amount = get_utilized_wallet_amount(conn) + get_unused_wallet_amount(um_futures_client)
    return total_wallet_amount


def get_wallet_utilization(conn, um_futures_client):
    utilized_wallet_amount = get_utilized_wallet_amount(conn)
    total_wallet_amount = get_total_wallet_amount(conn, um_futures_client)
    percent_utilization = float(utilized_wallet_amount / total_wallet_amount) * 100
    return percent_utilization


def send_email(subject, html, mail_config):
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = mail_config['FROM']
    message["To"] = mail_config['TO']
    message.attach(MIMEText(html, "html"))

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(mail_config['FROM'], mail_config['PASSWORD'])
        server.sendmail(
            mail_config['FROM'], mail_config['TO'], message.as_string()
        )


def main():
    # Set the config parameters using the config file
    current_time = datetime.utcnow()
    print(current_time)
    today = date.today()
    print("Today is: ", today)

    # Get 2 days earlier
    yesterday = today - timedelta(days=1)
    yesterday = yesterday.strftime('%Y-%m-%d')

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

    sms_text = ''
    email_summary = ''
    query = """select symbol, side, current_margin, net_pnl from positions p where date(updated_ts) = '{}' and position_status = 'CLOSED';""".format(yesterday)
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    total_pnl = 0.0
    index = 1
    for row in rows:
        symbol = row[0]
        side = row[1]
        current_margin = round(float(row[2]), 2)
        net_pnl = round(float(row[3]), 2)
        print(row)
        if side == "BUY":
            color_s = "green"
        else:
            color_s = "red"
        if net_pnl < 0:
            color_p = "red"
        else:
            color_p = "green"
        email_summary = email_summary + '<tr><td>{}</td> <td>{}</td> <td style="color:{}">{}</td> <td>{}</td> <td style="text-align:right;color:{}}">{}</td></tr>'.format(index, symbol, color_s, side, current_margin, color_p, net_pnl)
        total_pnl = total_pnl + float(row[1])
        index = index + 1


    if email_summary:
        sms_text = "Binance Futures Yesterday ({})'s Total PNL: {} \n".format(yesterday, str(round(float(total_pnl), 2)))
        sms_text = sms_text + "For detailed summary check mail."
        plivo_keys = get_db_details(connections_file, 'PLIVO_KEY')
        # send_sms(sms_text, plivo_keys, 'PLIVO')

        binance_keys = get_db_details(connections_file, 'BINANCE_KEY')
        um_futures_client = UMFutures(key=binance_keys['API_KEY'], secret=binance_keys['SECRET_KEY'])
        mail_config = get_db_details(connections_file, 'EMAIL')
        wallet_utilization = get_wallet_utilization(conn, um_futures_client)
        total_wallet_amount = get_total_wallet_amount(conn, um_futures_client)
        utilized_wallet_amount = get_utilized_wallet_amount(conn)
        unused_wallet_amount = get_unused_wallet_amount(um_futures_client)
        if total_pnl < 0:
            color_t = "red"
        else:
            color_t = "green"
        html = """\
        <html>
        <head>
            <style>
            table, th, td {{
              border: 1px solid black;
              border-collapse: collapse;
            }}
            </style>
            </head>
          <body>
            <p>Hi,<br><br>
               Summary for <b>Yesterday ({}):</b><br><br>
               <table>
                <tr>
                    <th>Index</th>
                    <th>Symbol</th>
                    <th>Side</th>
                    <th>Margin</th>
                    <th>Net PNL</th>
                </tr>
               {}
                <tr>
                    <td colspan="2"><b>Total</b></td>
                    <td style="text-align:right;color:{}">{}</td>
                </tr>
               </table><br>
               <table>
                <tr>
                    <th colspan="2">As of now - </th>
                </tr>
                <tr>
                    <td><b>Total Wallet Amount</b></td><td>{}</td>
                </tr>
                <tr>
                    <td><b>Utilized Wallet Amount</b></td><td>{}</td>
                </tr>
                <tr>    
                    <td><b>Unutilized Wallet Amount</b></td><td>{}</td>
                </tr>
                <tr>    
                    <td><b>Wallet Utilization</b></td><td>{}</td>
                </tr><br>
               Thanks
            </p>
          </body>
        </html>
        """.format(yesterday, email_summary, color_t, str(round(float(total_pnl), 2)), str(round(float(total_wallet_amount), 2)), str(round(float(utilized_wallet_amount), 2)), str(round(float(unused_wallet_amount), 2)), str(round(float(wallet_utilization), 2)))
        subject = "Binance Futures Summary for Yesterday {}".format(yesterday)
        print(html)
        send_email(subject, html, mail_config)
if __name__ == "__main__":
    main()
