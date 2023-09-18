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
import random
import requests

text_position = ''

def create_take_profit_order(symbol, position_id, current_margin, side, conn, um_futures_client, position_side,
                             is_repeat=False):
    # Create Take Profit Order
    exchange_info = get_exchange_info(symbol, um_futures_client)
    responses = um_futures_client.get_position_risk(symbol=symbol)
    for response in responses:
        if response['positionSide'] == position_side:
            entry_price = float(response['entryPrice'])
            leverage = int(response['leverage'])
            position_quantity = abs(float(response['positionAmt']))

    total_position_amount = entry_price * position_quantity

    # (25) % of margin is our profit
    profit = float((25 * current_margin) / 100)
    stop = float((24 * current_margin) / 100)

    if side == 'BUY':
        profit_position_amount = total_position_amount + profit
        stop_position_amount = total_position_amount + stop
        close_side = 'SELL'
    elif side == 'SELL':
        profit_position_amount = total_position_amount - profit
        stop_position_amount = total_position_amount - stop
        close_side = 'BUY'

    profit_closing_price = float(profit_position_amount / position_quantity)
    profit_closing_price = round_step_size(profit_closing_price, exchange_info['tickSize'])
    stop_price = float(stop_position_amount / position_quantity)
    stop_price = round_step_size(stop_price, exchange_info['tickSize'])
    logging.info("Symbol: %s, side: %s, Profit Closing Price: %s, Limit Closing Price: %s", symbol, close_side,
                 profit_closing_price, stop_price)
    try:
        response = um_futures_client.new_order(
            symbol=symbol,
            side=close_side,
            type="TAKE_PROFIT",
            stopPrice=stop_price,
            workingType='MARK_PRICE',
            quantity=position_quantity,
            price=profit_closing_price,
            positionSide=position_side
        )
    except ClientError as error:
        logging.info(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )
        response = um_futures_client.new_order(
            symbol=symbol,
            side=close_side,
            type="MARKET",
            positionSide=position_side,
            quantity=position_quantity
        )
        logging.info("Market order response from server.")
        logging.info(response)
        return

    logging.info("Profit order response from server.")
    logging.info(response)

    new_order_id = response['orderId']
    insert_order_record(symbol, position_id, new_order_id, conn, um_futures_client)


def get_order_pnl(symbol, order_id, um_futures_client):
    response = um_futures_client.get_account_trades(symbol=symbol, orderId=order_id)
    pnl = 0
    for item in response:
        pnl = pnl + float(item['realizedPnl'])
    return pnl


def get_order_fee(symbol, order_id, um_futures_client):
    response = um_futures_client.get_account_trades(symbol=symbol, orderId=order_id)
    fee = 0
    for item in response:
        fee = fee + float(item['commission'])
    return fee


def get_existing_positions(conn):
    sql = "select id, side from positions where position_status in ('OPEN', 'ALL_IN')"
    logging.info("Running the sql query: %s", sql)
    cursor = conn.cursor()
    cursor.execute(sql)

    position_ids = cursor.fetchall()
    cursor.close()
    position_ids = [(x[0], x[1]) for x in position_ids]
    logging.info("Fetched the following position ids")
    logging.info(position_ids)
    return position_ids


def close_and_update_order(symbol, order_id, src_order_id, status, conn, um_futures_client):
    current_time = datetime.now()
    current_timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')
    try:
        response = um_futures_client.query_order(symbol=symbol, orderId=src_order_id)
        if status == 'FILLED':

            fee = get_order_fee(symbol, src_order_id, um_futures_client)
            avg_price = float(response['avgPrice'])
            quantity = float(response['executedQty'])
            total_price = avg_price * quantity
            order_executed_time = datetime.fromtimestamp(int(response['updateTime']) / 1000).strftime(
                '%Y-%m-%d %H:%M:%S')
            update_ts = current_timestamp

            query = """update orders set avg_price = {}, total_price = {}, fee = {}, 
                    status = '{}', order_executed_time = '{}', updated_ts = '{}' where id = {}""". \
                format(avg_price, total_price, fee, 'FILLED', order_executed_time, update_ts, order_id)

        elif status == 'CANCEL':
            if response['status'] in ('NEW', 'PARTIALLY_FILLED'):
                um_futures_client.cancel_order(symbol=symbol, orderId=src_order_id)
                update_status = 'CANCELLED_BY_SYSTEM'
            else:
                update_status = 'CANCELLED_BY_USER'

            query = """update orders set status = '{}', updated_ts = '{}' where id = {}""". \
                format(update_status, current_timestamp, order_id)

        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()
        conn.commit()
    except ClientError as error:
        logging.info(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )


def get_total_fee(position_id, conn):
    sql = """ select sum(fee) from orders where position_id = {}""".format(position_id)
    cursor = conn.cursor()
    cursor.execute(sql)
    total_fee = cursor.fetchone()[0]
    cursor.close()
    return float(total_fee)


def check_current_status_and_update(position_id, conn, um_futures_client, position_side="BOTH"):
    current_time = datetime.now()
    current_timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')
    sql = """select id, symbol, side, leverage, starting_margin, current_margin, 
        entry_price, position_quantity, liquidation_price, manual_added_margin, position_status, closing_pnl,
        fee_incurred, net_pnl, created_ts, updated_ts, batch_id  from positions where id = {}""".format(position_id)

    cursor = conn.cursor()
    cursor.execute(sql)
    pos_data = cursor.fetchone()
    cursor.close()
    symbol = pos_data[1]
    if position_side == 'LONG':
        side = 'BUY'
    elif position_side == 'SHORT':
        side = 'SELL'
    leverage = int(pos_data[3])
    starting_margin = float(pos_data[4])
    manual_added_margin = pos_data[9]
    position_status = pos_data[10]
    # created_ts = datetime.strptime(pos_data[14], '%Y-%m-%d %H:%M:%S')
    # created_ts = pos_data[14]
    batch_id = pos_data[16]

    logging.info("Checking the status for following Position")
    logging.info("Position id: %s", position_id)
    logging.info("Symbol     : %s", symbol)

    logging.info("Getting Position risk from API.")
    try:
        response_risk = um_futures_client.get_position_risk(symbol=symbol)
    except ClientError as error:
        logging.info(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )
        logging.info("Unable to fetch position risk, Will retry later..")
        return
    for response in response_risk:
        if response['positionSide'] == position_side:
            current_margin = float(response['isolatedWallet'])
            entry_price = float(response['entryPrice'])
            position_quantity = abs(float(response['positionAmt']))
            liquidation_price = float(response['liquidationPrice'])

    profit_sql = """ select id, src_order_id from orders 
                            where position_id = {} and type = 'TAKE_PROFIT' and status = 'NEW'""".format(position_id)
    cursor = conn.cursor()
    cursor.execute(profit_sql)
    order_data = cursor.fetchone()
    cursor.close()
    profit_order_id = None
    profit_src_order_id = None
    if order_data is not None:
        profit_order_id = order_data[0]
        profit_src_order_id = order_data[1]

    create_opposite_position_flag = False
    create_same_position_flag = False

    global text_position
    if current_margin == 0.0:
        # position closed. Let's close the record in DB and update the PNL, fee and status and outstanding orders
        logging.info("Current Margin is 0.0. Checking if closed with Profit, Loss or Manually.")
        response_profit = {'status': 'CANCELLED'}
        # n = 3
        # while n > 0:
        if profit_src_order_id:
            logging.info("Getting Order information from API for Profit Order Id %s", profit_src_order_id)
            response_profit = um_futures_client.query_order(symbol=symbol, orderId=profit_src_order_id)
            logging.info("response_profit: %s", response_profit)
        #    n = n - 1
        #    time.sleep(1)

        # if filled
        if response_profit['status'] == 'FILLED':
            logging.info("Profit order id %s is filled. Position Closed on its own with Profit.", profit_src_order_id)
            logging.info("Cancelling the loss order.")
            if profit_order_id:
                close_and_update_order(symbol, profit_order_id, profit_src_order_id, 'FILLED', conn, um_futures_client)

            closing_order_id = profit_src_order_id

            # create_same_position_flag = True

        else:
            logging.info("Profit order id %s is not filled. Position Closed manually.", profit_src_order_id)
            logging.info("Cancelling the limit order and profit order.")
            if profit_order_id:
                close_and_update_order(symbol, profit_order_id, profit_src_order_id, 'CANCEL', conn, um_futures_client)

            response = um_futures_client.get_all_orders(symbol=symbol)
            if response[-1]['positionSide'] == position_side:
                closing_order_id = response[-1]['orderId']
            else:
                closing_order_id = response[-2]['orderId']
            logging.info("Position closed with order id %s.", closing_order_id)
            insert_order_record(symbol, position_id, closing_order_id, conn, um_futures_client)

        # Closing the position record

        pnl = get_order_pnl(symbol, closing_order_id, um_futures_client)
        total_fee = get_total_fee(position_id, conn)
        net_pnl = pnl - total_fee
        query = """update positions set closing_pnl = {}, fee_incurred = {}, net_pnl = {}, 
                        updated_ts = '{}', position_status = '{}' where id = {}""". \
            format(pnl, total_fee, net_pnl, current_timestamp, 'CLOSED', position_id)

        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()
        conn.commit()

        logging.info("Closing the position record.")
        logging.info("PNL    : %s", str(pnl))
        logging.info("FEE    : %s", str(total_fee))
        logging.info("NET_PNL: %s", str(net_pnl))
        text_position = text_position + str(symbol) + " closed with NET PNL " + str(
            round(net_pnl, 4)) + " on " + position_side + " side.\n"

        fetch_count = """ select count(1) from positions where batch_id = {} and symbol = '{}'""".format(batch_id, symbol)

        cursor = conn.cursor()
        cursor.execute(fetch_count)
        obj = cursor.fetchone()
        count = obj[0]
        logging.info('count: %s', count)
        cursor.close()

        if count < 3 and net_pnl > 0:
            logging.info("Creating a position again with %s side", position_side)
            create_position(batch_id, symbol, leverage, starting_margin, conn, um_futures_client, side=position_side)
        

def get_utilized_wallet_amount(conn):
    sql = "select coalesce(sum(current_margin), 0) from positions where position_status = 'OPEN'"

    cursor = conn.cursor()
    cursor.execute(sql)

    utilized_wallet_amount = float(cursor.fetchone()[0])

    return utilized_wallet_amount


def get_unused_wallet_amount(um_futures_client):
    account_info = um_futures_client.account()
    for asset in account_info['assets']:
        if asset['asset'] == 'BUSD':
            unused_amount = float(asset['maxWithdrawAmount']) + float(asset['openOrderInitialMargin'])
            return unused_amount


def get_total_wallet_amount(conn, um_futures_client):
    total_wallet_amount = get_utilized_wallet_amount(conn) + get_unused_wallet_amount(um_futures_client)
    return total_wallet_amount


def get_wallet_utilization(conn, um_futures_client):
    utilized_wallet_amount = get_utilized_wallet_amount(conn)
    total_wallet_amount = get_total_wallet_amount(conn, um_futures_client)
    percent_utilization = float(utilized_wallet_amount / total_wallet_amount) * 100
    return percent_utilization


def decide_side(symbol, um_futures_client):
    minute = int(datetime.now().time().strftime("%M"))
    if 0 <= minute % 15 <= 7:
        candles = um_futures_client.klines(symbol=symbol, interval="15m", limit=10)[:-1]
    else:
        candles = um_futures_client.klines(symbol=symbol, interval="15m", limit=9)
    open_for_range = float(candles[0][1])
    close_for_range = float(candles[-1][4])
    count = 0
    trends = []
    # print(candles[0])
    for candle in candles:
        # print(candle)
        open = float(candle[1])
        close = float(candle[4])
        per = (close - open) * 100 / open
        if per <= 0:
            count = count - 1
            trends.append(-1)
        else:
            count = count + 1
            trends.append(1)
        print(per)
    print(trends)
    range_per = (close_for_range - open_for_range) * 100 / open_for_range
    if -9 <= count <= -7:
        side = 'SELL'
    elif 7 <= count <= 9:
        side = 'BUY'
    elif -3 <= count <= 3:
        if range_per <= 0:
            if trends[-1] == 1 and trends[-2] == 1:
                side = 'BUY'
            else:
                side = 'SELL'
        else:
            if trends[-1] == -1 and trends[-2] == -1:
                side = 'SELL'
            else:
                side = 'BUY'
    elif -6 <= count <= -4:
        if range_per <= 0:
            if trends[-1] == 1 and trends[-2] == 1:
                side = 'BUY'
            else:
                side = 'SELL'
        else:
            side = 'SELL'
    elif 4 <= count <= 6:
        if range_per <= 0:
            if trends[-1] == -1 and trends[-2] == -1:
                side = 'SELL'
            else:
                side = 'BUY'
        else:
            side = 'BUY'
    # print(open_for_range)
    # print(high_for_range, index_high)
    # print(low_for_range, index_low)
    # print(close_for_range)
    # print(side)
    return side


def get_new_positions_symbols(total_new_positions, conn, um_futures_client):
    sql = "select symbol_name from symbols where is_active = 'Y' and symbol_name not in (select symbol from positions where position_status in ('OPEN', 'ALL_IN'))"
    cursor = conn.cursor()
    cursor.execute(sql)
    new_positions = cursor.fetchall()
    new_positions = [x[0] for x in new_positions]
    new_positions_selected = random.sample(new_positions, total_new_positions)
    return new_positions_selected


def insert_order_record(symbol, position_id, order_id, conn, um_futures_client):
    current_time = datetime.now()
    current_timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')
    logging.info("Inserting record for orderId %s", order_id)
    postgres_insert_query = """INSERT INTO orders (position_id, src_order_id, side, type, stop_price, avg_price, 
        quantity, total_price, fee, status, order_created_time, order_executed_time, created_ts, updated_ts) VALUES 
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """

    n_retry = 3
    while n_retry > 0:
        try:
            response = um_futures_client.query_order(symbol=symbol, orderId=order_id)
            break
        except ClientError as error:
            logging.info(
                "Found error. status: {}, error code: {}, error message: {}".format(
                    error.status_code, error.error_code, error.error_message
                )
            )
            logging.info("Retrying..")
            time.sleep(1)
            n_retry = n_retry - 1

    if n_retry == 0:
        logging.info("Retries failed. Continuing with next position")
        return

    side = response['side']
    type = response['type']
    stop_price = float(response['stopPrice'])
    avg_price = float(response['price'])
    quantity = float(response['origQty'])
    status = response['status']
    fee = None
    total_price = None
    order_created_time = datetime.fromtimestamp(int(response['time']) / 1000).strftime('%Y-%m-%d %H:%M:%S')
    order_executed_time = None

    if status == 'FILLED':
        fee = get_order_fee(symbol, order_id, um_futures_client)
        avg_price = float(response['avgPrice'])
        total_price = avg_price * quantity
        order_executed_time = datetime.fromtimestamp(int(response['updateTime']) / 1000).strftime('%Y-%m-%d %H:%M:%S')

    record_to_insert = (
        position_id, order_id, side, type, stop_price, avg_price, quantity, total_price, fee,
        status, order_created_time, order_executed_time, current_timestamp, current_timestamp)

    cursor = conn.cursor()
    cursor.execute(postgres_insert_query, record_to_insert)
    conn.commit()


def get_margin_type(symbol, um_futures_client):
    response = um_futures_client.get_position_risk(symbol=symbol)
    return response[0]["marginType"].upper()


def get_exchange_info(symbol, um_futures_client):
    response = um_futures_client.exchange_info()
    info = {}
    for item in response["symbols"]:
        if item["symbol"] == symbol:
            for symbol_filter in item['filters']:
                if symbol_filter['filterType'] == 'PRICE_FILTER':
                    info['tickSize'] = float(symbol_filter['tickSize'])
                if symbol_filter['filterType'] == 'LOT_SIZE':
                    info['stepSize'] = float(symbol_filter['stepSize'])
    return info


def round_step_size(quantity, step_size):
    quantity = Decimal(str(quantity))
    return float(quantity - quantity % Decimal(str(step_size)))


def get_tick_size(symbol, um_futures_client):
    info = um_futures_client.exchange_info()

    for symbol_info in info['symbols']:
        if symbol_info['symbol'] == symbol:
            for symbol_filter in symbol_info['filters']:
                if symbol_filter['filterType'] == 'PRICE_FILTER':
                    return float(symbol_filter['tickSize'])


def get_rounded_price(symbol, price, um_futures_client):
    return round_step_size(price, get_tick_size(symbol, um_futures_client))


def get_lot_size(symbol, um_futures_client):
    info = um_futures_client.exchange_info()

    for symbol_info in info['symbols']:
        if symbol_info['symbol'] == symbol:
            for symbol_filter in symbol_info['filters']:
                if symbol_filter['filterType'] == 'LOT_SIZE':
                    return float(symbol_filter['stepSize'])


def get_rounded_quantity(symbol, price, um_futures_client):
    return round_step_size(price, get_lot_size(symbol, um_futures_client))


def create_position(batch_id, symbol, leverage, each_position_amount, conn, um_futures_client, side="BOTH"):
    # leverage = 10
    exchange_info = get_exchange_info(symbol, um_futures_client)
    current_time = datetime.now()
    current_timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')

    max_l = um_futures_client.leverage_brackets(symbol=symbol)[0]['brackets'][0]['initialLeverage']
    leverage_actual = min(leverage, max_l)
    response = um_futures_client.change_leverage(symbol=symbol, leverage=leverage_actual)
    if get_margin_type(symbol, um_futures_client) == 'CROSS':
        response = um_futures_client.change_margin_type(symbol=symbol, marginType="ISOLATED")

    response = um_futures_client.mark_price(symbol=symbol)
    mark_price = float(response['markPrice'])
    
    if side == "BOTH":
        purchase_qty = float(((each_position_amount / 2) * leverage_actual) / mark_price)
    else:
        purchase_qty = float((each_position_amount * leverage_actual) / mark_price)
    
    purchase_qty = round_step_size(purchase_qty, exchange_info['stepSize'])

    postgres_insert_query = """INSERT INTO positions (symbol, side, leverage, starting_margin, current_margin,
                entry_price, position_quantity, liquidation_price, manual_added_margin, position_status, closing_pnl,
                fee_incurred, net_pnl, created_ts, updated_ts, batch_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """

    if side == "BOTH" or side == "LONG":
        response = um_futures_client.new_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            positionSide="LONG",
            quantity=purchase_qty,
        )
        new_order_id_long = response['orderId']

    if side == "BOTH" or side == "SHORT":
        response = um_futures_client.new_order(
            symbol=symbol,
            side="SELL",
            type="MARKET",
            positionSide="SHORT",
            quantity=purchase_qty,
        )
        new_order_id_short = response['orderId']

    responses = um_futures_client.get_position_risk(symbol=symbol)
    for response in responses:
        if response['positionSide'] == side or side == 'BOTH':
            entry_price = float(response['entryPrice'])
            starting_margin = float(response['isolatedWallet'])
            leverage = int(response['leverage'])
            position_side = response['positionSide']
            position_quantity = abs(float(response['positionAmt']))
            liquidation_price = float(response['liquidationPrice'])

            record_to_insert = (
                symbol, position_side, leverage_actual, starting_margin, starting_margin, entry_price,
                position_quantity,
                liquidation_price, 0,
                'OPEN', 0, 0, 0, current_timestamp, current_timestamp, batch_id)

            cursor = conn.cursor()
            cursor.execute(postgres_insert_query, record_to_insert)

            conn.commit()
            cursor.close()

    cursor = conn.cursor()
    position_id_query = "select id, starting_margin, side, position_quantity from positions where symbol = '{}' and position_status = 'OPEN'".format(
        symbol)
    cursor.execute(position_id_query)
    obj = cursor.fetchall()
    for row in obj:
        if row[2] == side or side == 'BOTH':
            position_id = row[0]
            starting_margin = float(row[1])
            position_side = row[2]
            position_quantity = float(row[3])
            if position_side == 'LONG':
                side_order = "BUY"
                insert_order_record(symbol, position_id, new_order_id_long, conn, um_futures_client)
            elif position_side == 'SHORT':
                side_order = "SELL"
                insert_order_record(symbol, position_id, new_order_id_short, conn, um_futures_client)

            # Create Take Profit Order
            create_take_profit_order(symbol, position_id, starting_margin, side_order, conn, um_futures_client, position_side)
            
            logging.info("Created following position")
            logging.info("Position Id: %s", str(position_id))
            logging.info("Symbol     : %s", str(symbol))
            logging.info("Leverage   : %s", str(leverage))
            logging.info("Margin     : %s", str(starting_margin))
            logging.info("Quantity   : %s", str(position_quantity))
            global text_position
            text_position = text_position + str(symbol) + " created with margin " + str(
                round(starting_margin, 4)) + " on " + position_side + " side.\n"


def check_and_update_symbols(conn, um_futures_client):
    current_time = datetime.now()
    current_timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')

    exchange_info = um_futures_client.exchange_info()
    incoming_symbol_list = []
    for position in exchange_info['symbols']:
        if position['symbol'].endswith('USDT') and position['status'] == 'TRADING' and position['symbol'] not in (
        "BTCUSDT", "ETHUSDT", "SOLUSDT"):
            incoming_symbol_list.append(position['symbol'])

    sql = "select symbol_name from symbols"
    cursor = conn.cursor()
    cursor.execute(sql)
    existing_symbols = cursor.fetchall()
    existing_symbols_list = [x[0] for x in existing_symbols]

    new_symbols = list(set(incoming_symbol_list) - set(existing_symbols_list))

    record_tuples = []
    for symbol in new_symbols:
        record = (symbol, 'Y', current_timestamp, current_timestamp)
        record_tuples.append(record)

    cursor = conn.cursor()
    cursor.executemany("INSERT INTO symbols VALUES(%s,%s,%s,%s)", record_tuples)
    conn.commit()

    inactive_symbols = list(set(existing_symbols_list) - set(incoming_symbol_list))
    if inactive_symbols:
        if len(inactive_symbols) == 1:
            sql = """update symbols set is_active = 'N', updated_ts = '{}' where symbol_name = '{}'""".format(
                current_timestamp, inactive_symbols[0])
        else:
            sql = """update symbols set is_active = 'N', updated_ts = '{}' where symbol_name in {}""".format(
                current_timestamp, tuple(inactive_symbols))
        cursor.execute(sql)
        conn.commit()

    common_symbols = list(set(existing_symbols_list) & set(incoming_symbol_list))
    if common_symbols:
        sql = """update symbols set is_active = 'Y', updated_ts = '{}' where symbol_name in {} and is_active = 'N'""".format(
            current_timestamp, tuple(common_symbols))
        cursor.execute(sql)
        conn.commit()


def create_new_positions(max_positions, conn, um_futures_client):
    # We will dynamically calculate the number of positions we need to create
    # formula - (ceil(total_wallet_amount / 200)) * 2
    current_time = datetime.now()
    batch_id = current_time.strftime('%Y%m%d%H%M')

    # total_wallet_amount = get_total_wallet_amount(conn, um_futures_client)
    # total_positions = (ceil(total_wallet_amount / 200)) * 2
    # total_positions = min(max_positions, total_positions)

    # sql_buy = "select coalesce(count(current_margin), 0) from positions where position_status = 'OPEN' and side = 'BUY'"
    # sql_sell = "select coalesce(count(current_margin), 0) from positions where position_status = 'OPEN' and side = 'SELL'"
    sql_open_pos = "select coalesce(count(current_margin), 0), DATEDIFF('hour', min(updated_ts)::timestamp, current_timestamp::timestamp) as hours_diff from positions where position_status = 'OPEN';"
    # update
    # global total_positions
    total_wallet_amount = get_total_wallet_amount(conn, um_futures_client)
    total_positions = (ceil(total_wallet_amount / 100)) * 2
    # total_positions = 4
    max_open_positions = total_positions * 2
    cursor = conn.cursor()
    cursor.execute(sql_open_pos)
    obj = cursor.fetchone()
    open_pos_count = obj[0]
    # day_diff = obj[1]
    hour_diff = obj[1]

    last_batch_query = "select sum(net_pnl), DATEDIFF('minute', max(updated_ts)::timestamp, current_timestamp::timestamp) as mins_diff from positions where batch_id = (select max(batch_id) from positions);"
    cursor.execute(last_batch_query)
    obj = cursor.fetchone()
    sum_pnl = obj[0]
    min_diff = obj[1]
    # cursor.execute(sql_buy)
    # buy_pos_count = cursor.fetchone()[0]

    # cursor.execute(sql_sell)
    # sell_pos_count = cursor.fetchone()[0]

    new_buy_pos_count = int(total_positions / 2)
    new_sell_pos_count = int(total_positions / 2)
    # close_pos_count = total_positions - open_pos_count
    # new_buy_pos_count = int(close_pos_count / 2)
    # new_sell_pos_count = close_pos_count - new_buy_pos_count
    # leverage = (ceil(total_wallet_amount / 150)) + 4
    # leverage = random.randint(10, 20)
    leverage = 5
    logging.info("open_pos_count: %s", open_pos_count)
    logging.info("total_positions: %s", total_positions)
    logging.info("min_diff: %s", min_diff)
    logging.info("total_wallet_amount: %s", total_wallet_amount)

    total_new_positions = total_positions - (ceil(open_pos_count / 2))
    logging.info("total_new_positions: %s", total_new_positions)
    # total_new_positions = 4
    if total_new_positions:
        new_positions_symbols = get_new_positions_symbols(total_new_positions, conn, um_futures_client)
        each_position_amount = float(total_wallet_amount * 60 / 100) / total_positions
        logging.info("each_position_amount: %s", each_position_amount)

        # each_position_amount = float(10)
        for symbol in new_positions_symbols:
            wallet_utilization = get_wallet_utilization(conn, um_futures_client)
            # if wallet_utilization < 30:
            create_position(batch_id, symbol, leverage, each_position_amount, conn, um_futures_client)
            # elif wallet_utilization >= 30:
            #    break


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


def time_is_between(time, time_range):
    if time_range[1] < time_range[0]:
        return time >= time_range[0] or time <= time_range[1]
    return time_range[0] <= time <= time_range[1]


def send_telegram(message, telegram_token):
    chat_id = '1061350969'
    api_url = f'https://api.telegram.org/bot{telegram_token}/sendMessage'

    try:
        response = requests.post(api_url, json={'chat_id': chat_id, 'text': message})
        print(response.text)
    except Exception as e:
        print(e)


def main():
    # Set the config parameters using the config file
    current_time = datetime.now()
    current_timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')

    home_dir = os.path.expanduser('~')
    workspace_dir = home_dir + '/binance-trader'
    config_file = workspace_dir + '/config/env_config.yaml'
    connections_file = home_dir + '/.secure/connections.yaml'
    configs = read_env(config_file)

    log_base_directory = configs["LOG_BASE_DIRECTORY"]
    database_identifier = configs["DATABASE_IDENTIFIER"]
    total_positions = configs["POSITIONS"]

    # Setting the Logging

    log_directory = log_base_directory + '/' + current_time.strftime('%Y%m%d')

    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    log_file_name = log_directory + '/Trader_' + current_time.strftime('%Y%m%d%H%M%S') + '.log'
    config_logging(logging, logging.INFO, log_file=log_file_name)
    logging.info("Trader Script Started at %s", current_timestamp)

    logging.info("Creating Database Connection")

    conn_details = get_db_details(connections_file, database_identifier)
    conn = psycopg2.connect(database=conn_details["DATABASE_NAME"],
                            user=conn_details["USER"], password=conn_details["PASSWORD"],
                            host=conn_details["HOST_NAME"], port=conn_details["PORT"]
                            )

    logging.info("Connection Details: ")
    logging.info(conn)

    logging.info("Creating Binance Connection")

    binance_keys = get_db_details(connections_file, 'BINANCE_KEY')
    um_futures_client = UMFutures(key=binance_keys['API_KEY'], secret=binance_keys['SECRET_KEY'])

    logging.info("Created Binance Connection")

    check_and_update_symbols(conn, um_futures_client)

    logging.info("Checking Existing Positions from Database")
    position_ids = get_existing_positions(conn)

    for position_id, side in position_ids:
        check_current_status_and_update(position_id, conn, um_futures_client, side)

    logging.info("Checking if we can create New Positions: ")

    wallet_utilization = get_wallet_utilization(conn, um_futures_client)
    logging.info("Wallet Utilization: %s", str(wallet_utilization))
    logging.info("Checking if new positions need to be created")
    max_positions = 20
    create_new_positions(max_positions, conn, um_futures_client)

    conn.commit()
    conn.close()

    global text_position

    if text_position:
        telegram_token = get_db_details(connections_file, 'TELEGRAM')['TOKEN']
        send_telegram(text_position, telegram_token)


if __name__ == "__main__":
    main()
