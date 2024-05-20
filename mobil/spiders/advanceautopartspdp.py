import scrapy
from scraper_api import ScraperAPIClient
import json
client = ScraperAPIClient('')

class AdvanceautopartspdpSpider(scrapy.Spider):
    def __init__(self, product_id=None, *args, **kwargs):
        super(AdvanceautopartspdpSpider, self).__init__(*args, **kwargs)
        self.product_id ='10275177'

    name = "advanceautopartspdp"
    allowed_domains = ["shop.advanceautoparts.com"]

    def start_requests(self):
        yield scrapy.Request(client.scrapyGet(f"https://shop.advanceautoparts.com/capi/v35/products?productIds={self.product_id}"))

    def parse(self, response):
        data_dict = json.loads(response.body)
        for data in data_dict.get('products'):
            yield{
                'asin': self.product_id,
                'name': data.get('productName'),
                'review_count': data.get('reviewsCount'),
                'description':data.get('description'),
                'price': data.get('regularPrice'),
            }
