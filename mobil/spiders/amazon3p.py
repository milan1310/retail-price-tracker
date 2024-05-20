import scrapy
from ..items import AmazonPDPItem
from scraper_api import ScraperAPIClient
client = ScraperAPIClient('')


class Amazon3pSpider(scrapy.Spider):
    def __init__(self, asin=None, *args, **kwargs):
        super(Amazon3pSpider, self).__init__(*args, **kwargs)
        self.asin =asin

    name = "amazon3p"
    allowed_domains = ["amazon.com"]

    def start_requests(self):
        yield scrapy.Request(client.scrapyGet(f"https://www.amazon.com/gp/product/ajax/ref=dp_aod_NEW_mbc?asin={self.asin}&m=&qid=&smid=&sourcecustomerorglistid=&sourcecustomerorglistitemid=&sr=&pc=dp&experienceId=aodAjaxMain"))

    def parse(self, response):
        offers = response.css('#aod-offer')
        for offer in offers:
            yield {
                'asin': self.asin,
                'title': offer.css("#aod-offer h5::text").get(),
                'price': f'{offer.css(".a-price-whole::text").get()}.{offer.css(".a-price-fraction::text").get()}', 
                'shipped_from': offer.css("#aod-offer-shipsFrom .a-color-base::text").get(),
                'sold_by': offer.css("#aod-offer-soldBy .a-link-normal::text").get(),
                # 'seller_store_url':f'https://www.amazon.com/{offer.css("#aod-offer-soldBy .a-link-normal::attr(href)").get()}'
            }
