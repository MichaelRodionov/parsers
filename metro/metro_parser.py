import json
import requests

from loguru import logger

import random

from config import data_directory_path, PROXIES


# --------------------------------------------------------------------------------------------------------------------
class MetroParser:
    def __init__(self, category_link: str):
        self.category_link = category_link
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'content-type': 'application/json',
            'origin': 'https://online.metro-cc.ru',
            'referer': 'https://online.metro-cc.ru/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }
        self.cookies = {'metroStoreId': '10'}
        self.goods = []

    def request_category(self, retry=5) -> list:
        """
        HTTP запрос категории
        """
        proxy = random.choice(PROXIES)
        proxies = {'http': f'http://{proxy}', 'https': f'http://{proxy}'}
        slug = self.category_link.split('/')[-1]
        json_data = {
            'query': '\n  query Query($storeId: Int!, $slug: String!, $attributes:[AttributeFilter], $filters: [FieldFilter], $from: Int!, $size: Int!, $sort: InCategorySort, $in_stock: Boolean, $eshop_order: Boolean, $is_action: Boolean, $priceLevelsOnline: Boolean) {\n    category (storeId: $storeId, slug: $slug, inStock: $in_stock, eshopAvailability: $eshop_order, isPromo: $is_action, priceLevelsOnline: $priceLevelsOnline) {\n      id\n      name\n      slug\n      id\n      parent_id\n      meta {\n        description\n        h1\n        title\n        keywords\n      }\n      disclaimer\n      description {\n        top\n        main\n        bottom\n      }\n      breadcrumbs {\n        category_type\n        id\n        name\n        parent_id\n        parent_slug\n        slug\n      }\n      promo_banners {\n        id\n        image\n        name\n        category_ids\n        type\n        sort_order\n        url\n        is_target_blank\n        analytics {\n          name\n          category\n          brand\n          type\n          start_date\n          end_date\n        }\n      }\n\n\n      dynamic_categories(from: 0, size: 9999) {\n        slug\n        name\n        id\n        category_type\n        dynamic_product_settings {\n          attribute_id\n          max_value\n          min_value\n          slugs\n          type\n        }\n      }\n      filters {\n        facets {\n          key\n          total\n          filter {\n            id\n            hru_filter_slug\n            is_hru_filter\n            is_filter\n            name\n            display_title\n            is_list\n            is_main\n            text_filter\n            is_range\n            category_id\n            category_name\n            values {\n              slug\n              text\n              total\n            }\n          }\n        }\n      }\n      total\n      prices {\n        max\n        min\n      }\n      pricesFiltered {\n        max\n        min\n      }\n      products(attributeFilters: $attributes, from: $from, size: $size, sort: $sort, fieldFilters: $filters)  {\n        health_warning\n        limited_sale_qty\n        id\n        slug\n        name\n        name_highlight\n        article\n        new_status\n        main_article\n        main_article_slug\n        is_target\n        category_id\n        category {\n          name\n        }\n        url\n        images\n        pick_up\n        rating\n        icons {\n          id\n          badge_bg_colors\n          rkn_icon\n          caption\n          type\n          is_only_for_sales\n          caption_settings {\n            colors\n            text\n          }\n          sort\n          image_svg\n          description\n          end_date\n          start_date\n          status\n        }\n        manufacturer {\n          name\n        }\n        packing {\n          size\n          type\n        }\n        stocks {\n          value\n          text\n          scale\n          eshop_availability\n          prices_per_unit {\n            old_price\n            offline {\n              price\n              old_price\n              type\n              offline_discount\n              offline_promo\n            }\n            price\n            is_promo\n            levels {\n              count\n              price\n            }\n            online_levels {\n              count\n              price\n              discount\n            }\n            discount\n          }\n          prices {\n            price\n            is_promo\n            old_price\n            offline {\n              old_price\n              price\n              type\n              offline_discount\n              offline_promo\n            }\n            levels {\n              count\n              price\n            }\n            online_levels {\n              count\n              price\n              discount\n            }\n            discount\n          }\n        }\n      }\n      argumentFilters {\n        eshopAvailability\n        inStock\n        isPromo\n        priceLevelsOnline\n      }\n    }\n  }\n',
            'variables': {
                'storeId': 10,
                'sort': 'default',
                'size': 10000,
                'from': 0,
                'filters': [
                    {
                        'field': 'main_article',
                        'value': '0',
                    },
                ],
                'attributes': [],
                'in_stock': False,
                'eshop_order': False,
                'allStocks': False,
                'slug': slug,
            },
        }
        try:
            response = requests.post(
                'https://api.metro-cc.ru/products-api/graph',
                headers=self.headers,
                json=json_data,
                proxies=proxies
            )
            logger.info(f'{response.status_code} | {slug}')
            if response.status_code == 200:
                json_ = response.json()
                products_raw = json_.get('data', {}).get('category', []).get('products', [])
                return products_raw
            else:
                if retry:
                    return self.request_category(retry - 1)
                return []
        except:
            if retry:
                return self.request_category(retry - 1)
            return []

    def parse_products(self, products_raw) -> None:
        """
        Метод скрапинга JSON с товарами
        """
        for product in products_raw:
            try:
                price, old_price = 0.0, 0.0
                price_ = product.get('stocks', [{}])[0].get('prices', {}).get('price', 0)
                if price_:
                    price = float(price_)
                old_price_ = product.get('stocks', [{}])[0].get('prices', {}).get('old_price', 0)
                if old_price_:
                    old_price = float(old_price_)
                prod = dict(
                    product_id=product.get('id'), product_name=product.get('name'),
                    link=f"https://online.metro-cc.ru{product.get('url')}",
                    price=price, old_price=old_price,
                    brand_name=str(product.get('manufacturer', {}).get('name', '-'))
                )
                stock = product.get('stocks', [{}])[0].get('value', 0)
                if prod not in self.goods:
                    if stock != 0:
                        self.goods.append(prod)
                    else:
                        logger.info(f'{prod.get("product_name")} | Количество: {stock} | Отсутствует в продаже')
                else:
                    logger.info(f'{prod.get("product_name")} | Дубликат')
            except Exception as e:
                logger.error(f"https://online.metro-cc.ru{product.get('url')} | {str(e)}")

    def main(self) -> list:
        """
        Основной метод парсера
        """
        if self.category_link:
            try:
                products_raw = self.request_category()
                self.parse_products(products_raw)
            except Exception as e:
                logger.error(str(e))


# ---------------------------------------------------------------------------------------------------------------------
def write_json(data):
    """
    Функция записи товаров в json
    """
    with open(f'{data_directory_path}/metro_products.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


# ---------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    logs_path = f'{data_directory_path}/logs/metro_logs.log'
    logger.add(logs_path, level='INFO')
    auchan_products = MetroParser(
        'https://online.metro-cc.ru/category/bezalkogolnye-napitki/pityevaya-voda-kulery'
    ).main()
    write_json(auchan_products)
