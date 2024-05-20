import re
import scrapy
from scraper_api import ScraperAPIClient
import json

client = ScraperAPIClient("")


class AutozonepdpSpider(scrapy.Spider):
    def __init__(self, product_id=None, *args, **kwargs):
        super(AutozonepdpSpider, self).__init__(*args, **kwargs)
        self.product_id = "835344"

    name = "autozonepdp"
    # allowed_domains = ["autozone.com", "api.bazaarvoice.com"]
    start_urls = ["https://autozone.com"]

    def start_requests(self):
        url = f"https://www.autozone.com/motor-oil-and-transmission-fluid/engine-oil/p/mobil-1-high-mileage-full-synthetic-engine-oil-5w-30-1-quart/785075_0_0?rrec=true"
        yield scrapy.Request(client.scrapyGet(url), callback=self.parse, meta={'original_url': url})

    def parse(self, response):
        # data_dict = json.loads(response.body)
        script_tags = response.css('script[type="application/ld+json"]::text').getall()
        for script in script_tags:
            data = json.loads(script)
            if data.get('@type') == 'Product':
                product_data = data
                if product_data:
                    yield {
                        "asin": self.extract_product_id(response.meta['original_url']),
                        "name": product_data.get("name"),
                        "brand": product_data.get("brand").get("name"),
                        "price":product_data.get('offers').get('price'),
                        "description":product_data.get('description'),
                    }
                break

    def extract_product_id(self,url):
        # Regex to find the product ID part of the URL before any query parameters
        match = re.search(r'/([^/?]+)(?:\?|$)', url)
        if match:
            return match.group(1)
        else:
            return None


    def handle_error(self, failure):
        print(failure)
        yield {"error": "error"}
