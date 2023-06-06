import asyncio
import asyncpg
from binance.client import AsyncClient
from config import *

api_key = key
api_secret = secret


async def insert_price(conn, exchange, eth_price, btc_price, eth_price_no_btc):
    try:
        insert_query = "INSERT INTO eth_btc_price (timestamp, exchange, eth_price, btc_price, eth_price_no_btc) VALUES (CURRENT_TIMESTAMP, $1, $2, $3, $4);"
        await conn.execute(insert_query, exchange, eth_price, btc_price, eth_price_no_btc)
    except (Exception, asyncpg.Error) as error:
        print("Ошибка при вставке цены в таблицу 'eth_btc_price':", error)


async def get_previous_eth_price(conn):
    try:
        query = "SELECT eth_price_no_btc FROM eth_btc_price WHERE timestamp >= NOW() - INTERVAL '1 hour' ORDER BY timestamp DESC LIMIT 1;"
        previous_price = await conn.fetchval(query)

        if previous_price is None:
            query = "SELECT eth_price_no_btc FROM eth_btc_price ORDER BY timestamp DESC LIMIT 1;"
            previous_price = await conn.fetchval(query)

        return previous_price

    except (Exception, asyncpg.Error) as error:
        if isinstance(error, asyncpg.UndefinedColumnError):
            print("Столбец 'eth_price_no_btc' не существует в таблице 'ETH_BTC'.")
        else:
            print("Ошибка при получении предыдущего значения eth_price_no_btc:", error)
        return None


def eth_price_excluding_bitcoin(eth_new_price, eth_old_price, btc_new_price, btc_old_price):
    eth_percentage_change = ((eth_new_price - eth_old_price) / eth_old_price) * 100
    btc_percentage_change = ((btc_new_price - btc_old_price) / btc_old_price) * 100
    difference = eth_percentage_change - btc_percentage_change
    eth_price_no_btc = (eth_new_price * difference) + eth_new_price
    return eth_price_no_btc


async def fetch_ticker(conn):
    client = await AsyncClient.create(api_key, api_secret)
    eth_old_price = None
    btc_old_price = None
    while True:
        ticker_eth = await client.futures_ticker(symbol='ETHUSDT')
        ticker_btc = await client.futures_ticker(symbol='BTCUSDT')

        eth_new_price = float(ticker_eth['lastPrice'])
        btc_new_price = float(ticker_btc['lastPrice'])
        exchange = "Binance"

        print("Цена фьючерса на Ethereum (ETHUSDT):", eth_new_price)
        print("Цена фьючерса на Bitcoin (BTCUSDT):", btc_new_price)

        if eth_old_price is None and btc_old_price is None:
            eth_old_price = eth_new_price
            btc_old_price = btc_new_price

        eth_price_no_btc = eth_price_excluding_bitcoin(eth_new_price, eth_old_price, btc_new_price, btc_old_price)

        hour_old_eth_price_no_btc = await get_previous_eth_price(conn)

        if hour_old_eth_price_no_btc is not None:
            hour_old_eth_price_no_btc = float(hour_old_eth_price_no_btc)
            if abs(((eth_price_no_btc - hour_old_eth_price_no_btc) / hour_old_eth_price_no_btc) * 100) >= 1:
                print("Цена фьючерса на Ethereum изменилась более 1% за последний час")
        else:
            print("Нет предыдущего значения eth_price_no_btc")

        eth_old_price = eth_new_price
        btc_old_price = btc_new_price

        await insert_price(conn, exchange, eth_new_price, btc_new_price, eth_price_no_btc)

        await asyncio.sleep(1)


async def main():
    conn = await asyncpg.connect(dsn=dsn)
    await conn.execute(create_table_query)
    await fetch_ticker(conn)
    await conn.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
