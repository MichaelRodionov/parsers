import asyncio
import json
import os
import platform

import aiohttp
from auchan.auchan_utils import get_cookies
from config import data_directory_path

from loguru import logger


# ----------------------------------------------------------------------------------------------------------------------
class AuchanParser:
    def __init__(self, category_link: str):
        self.category_link = category_link
        self.headers = {
            'accept': '*/*',
            'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'origin': 'https://www.auchan.ru',
            'referer': 'https://www.auchan.ru/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        }
        self.goods = []

    def get_products_from_json(self, products) -> None:
        """
        Метод скрапинга json и получения данных о товаре
        """
        for product_data in products:
            try:
                price = 0.0
                price_dict = product_data.get('price', {})
                if price_dict:
                    price = float(price_dict.get('value', 0.0))
                old_price_dict = product_data.get('oldPrice', {})
                if old_price_dict:
                    old_price = float(old_price_dict.get('value', 0.0))
                else:
                    old_price = price
                link = f"https://www.auchan.ru/product/{product_data.get('code')}/"
                prod = dict(
                    product_id=product_data.get('productId', ''),
                    product_name=str(product_data.get('title', '').replace('\xa0', ' ')), link=link, price=price,
                    old_price=old_price, brand_name=str(product_data.get('brand', {}).get('name', '')),
                )
                stock = product_data.get('stock', {}).get('qty', 0)
                if prod not in self.goods:
                    if stock != 0:
                        self.goods.append(prod)
                    else:
                        logger.info(f'{prod.get("product_name")} | Количество: {stock} | Отсутствует в продаже')
                else:
                    logger.info(f'{prod.get("name")} | Дубликат')
            except Exception as e:
                logger.info(str(e))

    async def request_category_products(self, session, proxy, cookies, page, repeat=False, retry=5):
        """
        Метод HTTP запроса категории с пагинацией
        """
        cookies['_GASHOP'] = '001_Mitishchi'
        cookies['region_id'] = '1'
        cookies['merchant_ID_'] = '1'
        params = {
            'merchantId': cookies['merchant_ID_'],
            'page': f'{page}',
            'perPage': '40',
            'deliveryAddressSelected': '0',
        }
        if not repeat:
            cat_param = self.category_link.split('/')[-2]
        else:
            cat_param = self.category_link.split('/')[-2].replace('-', '_')
        json_data = {
            'filter': {
                'category': f"{cat_param}",
                'promo_only': False,
                'active_only': False,
                'cashback_only': False,
            },
        }
        url = 'https://www.auchan.ru/v1/catalog/products'
        try:
            proxies = {'http': f'http://{proxy}', 'https': f'http://{proxy}'}
            response = await asyncio.wait_for(
                session.post(
                    url, headers=self.headers, cookies=cookies, proxy=proxies['http'], params=params, json=json_data
                ),
                timeout=session.timeout.sock_read
            )
            logger.info(f'{response.status} | {cat_param} | {page}')
            if response.status == 200:
                json_ = await response.json()
                products = json_.get('items', [])
                if page == 1:
                    if products:
                        self.get_products_from_json(products)
                    max_products = json_.get('activeRange', 0)
                    if max_products > 40:
                        max_page = int(max_products / 40) + 1
                    else:
                        max_page = 1
                    return max_page
                else:
                    self.get_products_from_json(products)
            elif response.status == 404:
                if retry:
                    return await self.request_category_products(
                        session, proxy, cookies, page, repeat=True, retry=retry - 1
                    )
            else:
                if retry:
                    return await self.request_category_products(session, proxy, cookies, page, retry - 1)
        except Exception as e:
            if retry:
                return await self.request_category_products(session, proxy, cookies, page, retry - 1)
            else:
                logger.error('Ошибка: ' + str(e))

    async def get_category_products_data(self):
        """
        Метод организации пула асинхронных задач для получения данных о продуктах с пагинацией
        """
        cookies, proxy = get_cookies()
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=15))
        try:
            task = asyncio.create_task(self.request_category_products(session, proxy, cookies, page=1))
            max_page = await task
            if max_page > 1:
                tasks = []
                for page in range(2, int(max_page) + 1):
                    task = asyncio.create_task(
                        self.request_category_products(session, proxy, cookies, page)
                    )
                    tasks.append(task)
                await asyncio.gather(*tasks)
            await session.close()
        except Exception as e:
            logger.info('Ошибка: ' + str(e))
            if session:
                await session.close()

    def main(self):
        if self.category_link:
            if platform.system() == 'Windows':
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            asyncio.run(self.get_category_products_data())
            return self.goods


# ---------------------------------------------------------------------------------------------------------------------
def write_json(data):
    """
    Функция записи товаров в json
    """
    with open(f'{data_directory_path}/auchan_products.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    logs_path = f'{data_directory_path}/logs/auchan_logs.log'
    logger.add(logs_path, level='INFO')
    auchan_products = AuchanParser(
        'https://www.auchan.ru/catalog/moloko-syr-yayca/moloko-slivki-molochnye-kokteyli-i-sguschennoe-moloko/'
    ).main()
    write_json(auchan_products)
    os.remove('proxy_auth.zip')
