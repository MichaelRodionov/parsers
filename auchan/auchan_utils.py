import json
import random
import time
import zipfile
from urllib.parse import urlparse

from loguru import logger
from lxml import html

from config import PROXIES
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


# ----------------------------------------------------------------------------------------------------------------------
class ChromeExtended(webdriver.Chrome):
    """
    Хромдрайвер для Selenoid
    """
    def __init__(self, proxy=None, **kwargs):
        options = Options()
        if proxy:
            parsed_proxy = urlparse(proxy)

            manifest_json = """
            {
                "version": "1.0.0",
                "manifest_version": 2,
                "name": "Chrome Proxy",
                "permissions": [
                    "proxy",
                    "tabs",
                    "unlimitedStorage",
                    "storage",
                    "<all_urls>",
                    "webRequest",
                    "webRequestBlocking"
                ],
                "background": {
                    "scripts": ["background.js"]
                },
                "minimum_chrome_version":"22.0.0"
            }
            """
            background_js = """
                var config = {
                        mode: "fixed_servers",
                        rules: {
                        singleProxy: {
                            scheme: "http",
                            host: "%s",
                            port: parseInt(%s)
                        },
                        bypassList: ["localhost"]
                        }
                    };

                chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

                function callbackFn(details) {
                    return {
                        authCredentials: {
                            username: "%s",
                            password: "%s"
                        }
                    };
                }

                chrome.webRequest.onAuthRequired.addListener(
                            callbackFn,
                            {urls: ["<all_urls>"]},
                            ['blocking']
                );
                """ % (parsed_proxy.hostname, parsed_proxy.port, parsed_proxy.username, parsed_proxy.password)

            path_ = 'proxy_auth.zip'
            with zipfile.ZipFile(path_, "w") as f:
                f.writestr("manifest.json", manifest_json)
                f.writestr("background.js", background_js)

            options.add_extension(path_)
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-notifications')
        options.capabilities['browserName'] = 'chrome'
        super().__init__(options=options, **kwargs)


def get_cookies(retry=5):
    try:
        proxy = random.choice(PROXIES)
        driver = ChromeExtended(proxy)
        driver.get('https://www.auchan.ru/')
        driver.execute_script("window.open('https://www.auchan.ru/', '_blank')")
        time.sleep(5)
        cookies_data = driver.get_cookies()
        cookies = {}
        for cookie in cookies_data:
            cookies.update({cookie['name']: cookie['value']})
        driver.quit()
        if cookies:
            return cookies, proxy
        else:
            if retry:
                return get_cookies(retry - 1)
    except:
        if retry:
            return get_cookies(retry - 1)


def get_json_from_html(html_list, search_tag='window.__INITIAL_STATE__', type_=None, pid=None):
    """
    Метод для получения json из html страницы
    """
    page = 0
    if type_ == 'product':
        html_list_, page = html_list
    elif type_ == 'barcode':
        html_list_, link = html_list
    else:
        html_list_ = html_list
    if html_list_:
        tree = html.fromstring(html_list_)
        script_tags = tree.xpath('//script')
        script_with_json = None
        if script_tags:
            for script_tag in script_tags:
                text_ = script_tag.text_content()
                if text_ and search_tag in text_:
                    script_with_json = text_
                    break
        if script_with_json:
            try:
                dict_ = script_with_json.split('window.__INITIAL_STATE__ = ')[-1]
                json_data = json.loads(dict_)
                if type_ == 'category':
                    categories = json_data.get('categories', {}).get('categories')
                    return categories
                elif type_ == 'shop':
                    shops = json_data.get('shops', {}).get('shopsList')
                    if shops:
                        return shops
                elif type_ == 'product':
                    products = json_data.get('products', {}).get('products')
                    if products:
                        return products, page
                elif type_ == 'barcode':
                    product = json_data.get('product', {}).get('product')
                    if product:
                        return product
            except Exception as e:
                if pid:
                    logger.error(f'PID: {pid} | Ошибка: {str(e)}')
                else:
                    logger.error(f'Ошибка: {str(e)}')
    else:
        if type_ == 'category':
            return []
