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


text_position = ''




def create_limit_order(symbol, position_id, starting_margin, current_margin, side, conn, um_futures_client):

    # Create New Order for 25% loss for 50% addition of original margin
    # Update - Create New Order for 50% loss for 100% addition of original margin
    # Update - Create New Order for 20% loss for 40% addition of original margin
    exchange_info = get_exchange_info(symbol, um_futures_client)
    response = um_futures_client.get_position_risk(symbol=symbol)
    entry_price = float(response[0]['entryPrice'])
    leverage = int(response[0]['leverage'])
    position_quantity = abs(float(response[0]['positionAmt']))
    total_position_amount = entry_price * position_quantity

    # loss = float(current_margin / 4)
    # loss = float(current_margin / 2)
    loss = float(current_margin / 5)

    if side == 'BUY':
        loss_position_amount = total_position_amount - loss
    elif side == 'SELL':
        loss_position_amount = total_position_amount + loss

    loss_mark_price = float(loss_position_amount / position_quantity)
    loss_mark_price = round_step_size(loss_mark_price, exchange_info['tickSize'])
    # margin_to_add = float(starting_margin / 2)
    # margin_to_add = float(starting_margin)
    margin_to_add = float(starting_margin / 2.5)

    purchase_qty = float((margin_to_add * leverage) / loss_mark_price)
    purchase_qty = round_step_size(purchase_qty, exchange_info['stepSize'])

    response_mp = um_futures_client.mark_price(symbol=symbol)
    mark_price = float(response_mp['markPrice'])

    if (side == 'BUY' and mark_price < loss_mark_price) or (side == 'SELL' and mark_price > loss_mark_price):
        loss_mark_price = round_step_size(loss_mark_price, exchange_info['tickSize'])

    logging.info("Symbol: %s, side: %s, Purchase Qty: %s, Loss Mark Price: %s", symbol, side, purchase_qty, loss_mark_price)
    response = um_futures_client.new_order(
        symbol=symbol,
        side=side,
        type="LIMIT",
        quantity=purchase_qty,
        timeInForce="GTC",
        price=loss_mark_price
    )
    logging.info("Limit order response from server.")
    logging.info(response)
    new_order_id = response['orderId']
    insert_order_record(symbol, position_id, new_order_id, conn, um_futures_client)


def create_profit_order(symbol, position_id, starting_margin, current_margin, side, conn, um_futures_client, strategy='NORMAL'):

    # Create Take Profit Order
    exchange_info = get_exchange_info(symbol, um_futures_client)
    response = um_futures_client.get_position_risk(symbol=symbol)
    entry_price = float(response[0]['entryPrice'])
    position_quantity = abs(float(response[0]['positionAmt']))
    total_position_amount = entry_price * position_quantity

    # 10% of margin is our profit
    # Update - ratio of current margin and starting margin into 10 is our profit
    # Update - 15% of margin is our profit for Normal Strategy, 50% of margin is our profit for Express Strategy
    # ratio = float(current_margin/starting_margin)
    # profit = float(starting_margin / 10)
    # profit = float((ratio * starting_margin) / 10)
    if strategy == 'NORMAL':
        profit = float((15 * current_margin) / 100)
    elif strategy == 'EXPRESS':
        profit = float((50 * current_margin) / 100)

    if side == 'BUY':
        profit_position_amount = total_position_amount + profit
        close_side = 'SELL'
    elif side == 'SELL':
        profit_position_amount = total_position_amount - profit
        close_side = 'BUY'

    profit_closing_price = float(profit_position_amount / position_quantity)
    profit_closing_price = round_step_size(profit_closing_price, exchange_info['tickSize'])
    logging.info("Symbol: %s, side: %s, Profit Closing Price: %s", symbol, close_side, profit_closing_price)
    response = um_futures_client.new_order(
        symbol=symbol,
        side=close_side,
        type="TAKE_PROFIT_MARKET",
        stopPrice=profit_closing_price,
        closePosition=True,
        workingType='MARK_PRICE'
    )

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
    sql = "select id from positions where position_status in ('OPEN', 'ALL_IN')"
    logging.info("Running the sql query: %s", sql)
    cursor = conn.cursor()
    cursor.execute(sql)

    position_ids = cursor.fetchall()
    cursor.close()
    position_ids = [x[0] for x in position_ids]
    logging.info("Fetched the following position ids")
    logging.info(position_ids)
    return position_ids


def close_and_update_order(symbol, order_id, src_order_id, status, conn, um_futures_client):
    current_time = datetime.utcnow()
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


def check_current_status_and_update(position_id, conn, um_futures_client):
    current_time = datetime.utcnow()
    current_timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')
    sql = """select id, symbol, side, leverage, starting_margin, current_margin, 
        entry_price, position_quantity, liquidation_price, manual_added_margin, position_status, closing_pnl,
        fee_incurred, net_pnl, created_ts, updated_ts  from positions where id = {}""".format(position_id)

    cursor = conn.cursor()
    cursor.execute(sql)
    pos_data = cursor.fetchone()
    cursor.close()
    symbol = pos_data[1]
    side = pos_data[2]
    leverage = pos_data[3]
    starting_margin = pos_data[4]
    manual_added_margin = pos_data[9]
    position_status = pos_data[10]
    created_ts = datetime.strptime(pos_data[14], '%Y-%m-%d %H:%M:%S')

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
    current_margin = float(response_risk[0]['isolatedWallet'])
    entry_price = float(response_risk[0]['entryPrice'])
    position_quantity = abs(float(response_risk[0]['positionAmt']))
    liquidation_price = float(response_risk[0]['liquidationPrice'])

    profit_sql = """ select id, src_order_id from orders 
                            where position_id = {} and type = 'TAKE_PROFIT_MARKET' and status = 'NEW'""".format(position_id)
    cursor = conn.cursor()
    cursor.execute(profit_sql)
    order_data = cursor.fetchone()
    cursor.close()
    if order_data is not None:
        profit_order_id = order_data[0]
        profit_src_order_id = order_data[1]

    limit_sql = """ select id, src_order_id from orders 
                                    where position_id = {} and type = 'LIMIT' and status = 'NEW'""".format(position_id)
    cursor = conn.cursor()
    cursor.execute(limit_sql)
    order_data = cursor.fetchone()
    cursor.close()
    limit_order_id = None
    limit_src_order_id = None
    if order_data is not None:
        limit_order_id = order_data[0]
        limit_src_order_id = order_data[1]

    current_pnl_percentage = 0.0
    if current_margin != 0.0:
        current_pnl_percentage = float(float(response_risk[0]['unRealizedProfit']) / current_margin) * 100
    hours_diff = (current_time - created_ts).total_seconds() / 3600

    global text_position
    if current_margin == 0.0 or (((5 < current_pnl_percentage) and hours_diff >= 36) or ((-10 < current_pnl_percentage < 0) and hours_diff >= 48)):
        # position closed. Let's close the record in DB and update the PNL, fee and status and outstanding orders
        if current_margin == 0.0:
            logging.info("Current Margin is 0.0. Position is closed.")
            response = {'status': 'CANCELLED'}
            if profit_src_order_id:
                logging.info("Getting Order information from API for Profit Order Id %s", profit_src_order_id)
                response = um_futures_client.query_order(symbol=symbol, orderId=profit_src_order_id)

            # if filled
            if response['status'] == 'FILLED':
                logging.info("Profit order id %s is filled. Position Closed on its own.", profit_src_order_id)
                logging.info("Cancelling the limit order.")
                if profit_order_id:
                    close_and_update_order(symbol, profit_order_id, profit_src_order_id, 'FILLED', conn, um_futures_client)
                if limit_order_id:
                    close_and_update_order(symbol, limit_order_id, limit_src_order_id, 'CANCEL', conn, um_futures_client)
                closing_order_id = profit_src_order_id
            else:
                logging.info("Profit order id %s is not filled. Position Closed manually.", profit_src_order_id)
                logging.info("Cancelling the limit order and profit order.")
                if profit_order_id:
                    close_and_update_order(symbol, profit_order_id, profit_src_order_id, 'CANCEL', conn, um_futures_client)
                if limit_order_id:
                    close_and_update_order(symbol, limit_order_id, limit_src_order_id, 'CANCEL', conn, um_futures_client)

                response = um_futures_client.get_all_orders(symbol=symbol)
                closing_order_id = response[-1]['orderId']
                logging.info("Position closed with order id %s.", closing_order_id)
                insert_order_record(symbol, position_id, closing_order_id, conn, um_futures_client)
        else:
            logging.info("Hours Passed since Creation of this position is %s and current_pnl_percentage is %s", hours_diff, current_pnl_percentage)
            logging.info("hence force closing the order since this is in our force close criteria")
            if side == 'BUY':
                close_side = 'SELL'
            elif side == 'SELL':
                close_side = 'BUY'
            response = um_futures_client.new_order(
                symbol=symbol,
                side=close_side,
                type="MARKET",
                reduceOnly=True,
                quantity=position_quantity
            )
            closing_order_id = response['orderId']
            logging.info("Position closed with order id %s.", closing_order_id)
            insert_order_record(symbol, position_id, closing_order_id, conn, um_futures_client)
            if profit_order_id:
                close_and_update_order(symbol, profit_order_id, profit_src_order_id, 'CANCEL', conn, um_futures_client)
            if limit_order_id:
                close_and_update_order(symbol, limit_order_id, limit_src_order_id, 'CANCEL', conn, um_futures_client)

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

        text_position = text_position + str(symbol) + " closed with NET PNL " + str(round(net_pnl, 2)) + "\n"

    else:
        logging.info("Position is not closed.")
        if  leverage == 10:
            if limit_src_order_id is not None:
                response = um_futures_client.query_order(symbol=symbol, orderId=limit_src_order_id)
                if response['status'] == 'FILLED':
                    logging.info("Limit order id %s is filled", limit_src_order_id)
                    close_and_update_order(symbol, limit_order_id, limit_src_order_id, 'FILLED', conn, um_futures_client)
                    close_and_update_order(symbol, profit_order_id, profit_src_order_id, 'CANCEL', conn, um_futures_client)
                    logging.info("Closing the previous profit order and creating new for updated quantity")
                    create_profit_order(symbol, position_id, starting_margin, current_margin, side, conn, um_futures_client)
                    ratio = float(current_margin / starting_margin)
                    if ratio < 2.1:
                        logging.info("Ratio of current margin and starting margin is %s which is less than 2.2, hence creating another limit order.", str(ratio))
                        create_limit_order(symbol, position_id, starting_margin, current_margin, side, conn, um_futures_client)
                    else:
                        logging.info(
                            "Ratio of current margin and starting margin is %s, hence not creating another limit order. We will add manual margin once the loss percentage goes beyond 60", ratio)
                    logging.info("Position updated with following")
                    logging.info("Position Id: %s", str(position_id))
                    logging.info("Symbol     : %s", str(symbol))
                    logging.info("Margin     : %s", str(current_margin))
                    logging.info("Quantity   : %s", str(position_quantity))
                    # text_position = text_position + str(symbol) + " updated with limit margin " + str(round(current_margin, 2)) + "\n"
            elif limit_src_order_id is None:
                ratio = float(current_margin / starting_margin)
                if ratio < 2.1:
                    logging.info(
                        "Ratio of current margin and starting margin is not 3, hence creating another limit order.")
                    create_limit_order(symbol, position_id, starting_margin, current_margin, side, conn, um_futures_client)
                else:
                    logging.info(
                        "Ratio of current margin and starting margin is %s, hence not creating another limit order. We will add manual margin once the loss percentage goes beyond 60", ratio)
                if ratio < 2.9:
                    if current_pnl_percentage < -60.0:
                        um_futures_client.modify_isolated_position_margin(symbol=symbol, amount=float(starting_margin / 2.5), type=1)
                        logging.info(
                            "Ratio of current margin and starting margin is %s, which is less than 3 and current_pnl_percentage is %s, hence adding manual margin",
                            str(ratio), str(current_pnl_percentage))
                        current_margin = current_margin + float(starting_margin / 2.5)
                        manual_added_margin = float(manual_added_margin) + float(starting_margin / 2.5)
                        # text_position = text_position + str(symbol) + " updated with manual margin " + str(round(current_margin, 2)) + "\n"
                else:
                    logging.info("Ratio of current margin and starting margin is greater than %s, doing nothing... just waiting", str(ratio))
                    position_status = "ALL_IN"

            query = """update positions set current_margin = {}, entry_price = {}, position_quantity = {}, manual_added_margin = {},
                                            liquidation_price = {}, updated_ts = '{}', position_status = '{}' where id = {}""". \
                format(current_margin, entry_price, position_quantity, manual_added_margin, liquidation_price, current_timestamp, position_status,
                       position_id)

            cursor = conn.cursor()
            cursor.execute(query)
            cursor.close()
            conn.commit()


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


def decide_side(symbol, um_futures_client):
    candles = um_futures_client.klines(symbol=symbol, interval="15m", limit=9)
    candles = candles[:-1]
    open_for_range = float(candles[0][1])
    close_for_range = float(candles[-1][4])
    high_for_range = float(candles[0][1])
    low_for_range = float(candles[-1][4])
    i = 0
    index_low = 0
    index_high = 0
    # print(candles[0])
    for candle in candles:
        # print(candle)
        if high_for_range < float(candle[2])  and i != 0:
            high_for_range = float(candle[2])
            index_high = i
        if low_for_range > float(candle[3]) and i != len(candles) -1:
            low_for_range = float(candle[3])
            index_low = i
        i = i + 1
    # print(candles[-1])
    if open_for_range > close_for_range:
        if open_for_range == high_for_range:
            if close_for_range == low_for_range:
                side = 'SELL'
            elif close_for_range > low_for_range:
                side = 'BUY'
        elif open_for_range < high_for_range:
            if close_for_range == low_for_range:
                side = 'SELL'
            elif close_for_range > low_for_range:
                if index_high < index_low:
                    side = 'BUY'
                else:
                    side = 'SELL'
    elif open_for_range <= close_for_range:
        if open_for_range == low_for_range:
            if close_for_range == high_for_range:
                side = 'BUY'
            elif close_for_range < high_for_range:
                side = 'SELL'
        elif open_for_range > low_for_range:
            if close_for_range == high_for_range:
                side = 'BUY'
            elif close_for_range < high_for_range:
                if index_high > index_low:
                    side = 'SELL'
                else:
                    side = 'BUY'
    # print(open_for_range)
    # print(high_for_range, index_high)
    # print(low_for_range, index_low)
    # print(close_for_range)
    # print(side)
    return side

def get_new_positions_symbols(total_new_positions, new_buy_pos_count, new_sell_pos_count, conn, um_futures_client):
    sql = "select symbol_name from symbols where is_active = 'Y' and symbol_name not in (select symbol from positions where position_status in ('OPEN', 'ALL_IN') or (DATE_PART('day', current_timestamp::timestamp - created_ts::timestamp) = 1 and position_status = 'CLOSED'))"
    cursor = conn.cursor()
    cursor.execute(sql)
    new_positions = cursor.fetchall()
    new_positions = [x[0] for x in new_positions]
    new_positions_selected = random.sample(new_positions, total_new_positions)
    new_positions_ordered = {}
    l = ['BUY' for i in range(new_buy_pos_count)] + ['SELL' for i in range(new_sell_pos_count)]
    random.shuffle(l)
    for i in range(total_new_positions):
        symbol = new_positions_selected.pop()
        side = decide_side(symbol, um_futures_client)
        new_positions_ordered[symbol] = side

    return new_positions_ordered


def insert_order_record(symbol, position_id, order_id, conn, um_futures_client):
    current_time = datetime.utcnow()
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
    order_created_time = datetime.fromtimestamp(int(response['time'])/1000).strftime('%Y-%m-%d %H:%M:%S')
    order_executed_time = None

    if status == 'FILLED':
        fee = get_order_fee(symbol, order_id, um_futures_client)
        avg_price = float(response['avgPrice'])
        total_price = avg_price * quantity
        order_executed_time = datetime.fromtimestamp(int(response['updateTime'])/1000).strftime('%Y-%m-%d %H:%M:%S')

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

def create_position(symbol, side, each_position_amount, conn, um_futures_client, strategy):

    if strategy == 'NORMAL':
        leverage = 10
    elif strategy == 'EXPRESS':
        leverage = 20

    exchange_info = get_exchange_info(symbol, um_futures_client)
    current_time = datetime.utcnow()
    current_timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')

    response = um_futures_client.change_leverage(symbol=symbol, leverage=leverage)
    if get_margin_type(symbol, um_futures_client) == 'CROSS':
        response = um_futures_client.change_margin_type(symbol=symbol, marginType="ISOLATED")

    response = um_futures_client.mark_price(symbol=symbol)
    mark_price = float(response['markPrice'])
    purchase_qty = float((each_position_amount * leverage) / mark_price)
    purchase_qty = round_step_size(purchase_qty, exchange_info['stepSize'])

    response = um_futures_client.new_order(
        symbol=symbol,
        side=side,
        type="MARKET",
        quantity=purchase_qty,
    )

    new_order_id = response['orderId']
    postgres_insert_query = """INSERT INTO positions (symbol, side, leverage, starting_margin, current_margin,
        entry_price, position_quantity, liquidation_price, manual_added_margin, position_status, closing_pnl,
        fee_incurred, net_pnl, created_ts, updated_ts) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
    response = um_futures_client.get_position_risk(symbol=symbol)
    entry_price = float(response[0]['entryPrice'])
    starting_margin = float(response[0]['isolatedWallet'])
    leverage = int(response[0]['leverage'])
    position_quantity = abs(float(response[0]['positionAmt']))
    liquidation_price = float(response[0]['liquidationPrice'])

    record_to_insert = (
        symbol, side, leverage, starting_margin, starting_margin, entry_price, position_quantity, liquidation_price, 0,
        'OPEN', 0, 0, 0, current_timestamp, current_timestamp)

    cursor = conn.cursor()
    cursor.execute(postgres_insert_query, record_to_insert)

    conn.commit()

    position_id_query = "select id from positions where symbol = '{}' and position_status = 'OPEN'".format(symbol)
    cursor.execute(position_id_query)
    position_id = cursor.fetchone()[0]

    insert_order_record(symbol, position_id, new_order_id, conn, um_futures_client)

    # Create Take Profit Order
    create_profit_order(symbol, position_id, starting_margin, starting_margin, side, conn, um_futures_client, strategy)

    # Create limit order
    if strategy == 'NORMAL':
        create_limit_order(symbol, position_id, starting_margin, starting_margin, side, conn, um_futures_client)

    logging.info("Created following position")
    logging.info("Position Id: %s", str(position_id))
    logging.info("Symbol     : %s", str(symbol))
    logging.info("Leverage   : %s", str(leverage))
    logging.info("Margin     : %s", str(starting_margin))
    logging.info("Quantity   : %s", str(position_quantity))

    global text_position
    # text_position = text_position + str(symbol) + " created with margin " + str(round(starting_margin, 2)) + "\n"


def check_and_update_symbols(conn, um_futures_client):

    current_time = datetime.now()
    current_timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')

    exchange_info = um_futures_client.exchange_info()
    incoming_symbol_list = []
    for position in exchange_info['symbols']:
        if position['symbol'].endswith('USDT') and position['status'] == 'TRADING':
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
        sql = """update symbols set is_active = 'N', updated_ts = '{}' where symbol_name in {}""".format(current_timestamp, tuple(inactive_symbols))
        cursor.execute(sql)
        conn.commit()

    common_symbols = list(set(existing_symbols_list) & set(incoming_symbol_list))
    if common_symbols:
        sql = """update symbols set is_active = 'Y', updated_ts = '{}' where symbol_name in {} and is_active = 'N'""".format(current_timestamp, tuple(common_symbols))
        cursor.execute(sql)
        conn.commit()


def create_new_positions(max_positions, conn, um_futures_client, strategy):
    # We will dynamically calculate the number of positions we need to create
    # formula - (ceil(total_wallet_amount / 200)) * 2

    total_wallet_amount = get_total_wallet_amount(conn, um_futures_client)
    total_positions = (ceil(total_wallet_amount / 200)) * 2
    total_positions = min(max_positions, total_positions)

    sql_buy = "select coalesce(count(current_margin), 0) from positions where position_status = 'OPEN' and side = 'BUY'"
    sql_sell = "select coalesce(count(current_margin), 0) from positions where position_status = 'OPEN' and side = 'SELL'"

    # update
    total_positions = 16
    cursor = conn.cursor()

    cursor.execute(sql_buy)
    buy_pos_count = cursor.fetchone()[0]

    cursor.execute(sql_sell)
    sell_pos_count = cursor.fetchone()[0]

    new_buy_pos_count = int(total_positions / 2) - buy_pos_count
    new_sell_pos_count = int(total_positions / 2) - sell_pos_count

    total_new_positions = new_buy_pos_count + new_sell_pos_count
    if total_new_positions > 0:
        new_positions_symbols = get_new_positions_symbols(total_new_positions, new_buy_pos_count, new_sell_pos_count, conn, um_futures_client)
        total_wallet_amount = get_total_wallet_amount(conn, um_futures_client)
        if strategy == 'NORMAL':
            each_position_amount = float(total_wallet_amount / 5) / total_positions
        elif strategy == 'EXPRESS':
            each_position_amount = 5.0

        for symbol, side in new_positions_symbols.items():
            wallet_utilization = get_wallet_utilization(conn, um_futures_client)
            if (wallet_utilization < 30 and strategy == 'NORMAL') or strategy == 'EXPRESS':
                create_position(symbol, side, each_position_amount, conn, um_futures_client, strategy)
            elif wallet_utilization >= 30:
                break




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

def main():
    # Set the config parameters using the config file
    n = len(sys.argv)
    if n != 2:
        print("Argument Expected - 'NORMAL' or 'EXPRESS' for strategy")
        exit(1)

    strategy = sys.argv[1]
    if strategy not in ['NORMAL', 'EXPRESS']:
        print("Argument Expected - 'NORMAL' or 'EXPRESS' for strategy")

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

    log_file_name = log_directory + '/Trader_' + strategy + '_' + current_time.strftime('%Y%m%d%H%M%S') + '.log'
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

    for position_id in position_ids:
        try:
            check_current_status_and_update(position_id, conn, um_futures_client)
        except Exception as e:
            print(e)
            continue

    logging.info("Checking if we can create New Positions: ")

    wallet_utilization = get_wallet_utilization(conn, um_futures_client)
    logging.info("Wallet Utilization: %s", str(wallet_utilization))
    logging.info("Checking if new positions need to be created")
    max_positions = 20
    # create_new_positions(max_positions, conn, um_futures_client, strategy)

    conn.commit()
    conn.close()

    global text_position

    # if text_position and not time_is_between(datetime.utcnow().time().strftime("%H:%M"), ("18:30", "02:30")):
    #     twilio_keys = get_db_details(connections_file, 'TWILIO_KEY')
    #     send_sms(text_position, twilio_keys)


if __name__ == "__main__":
    main()
