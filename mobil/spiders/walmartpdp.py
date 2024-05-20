import scrapy
from scraper_api import ScraperAPIClient
import urllib.parse
import re
import json
from ..items import WalmartPDPItem

client = ScraperAPIClient('')
ScraperAPI = True
ScrapOPS = False


class WalmartpdpSpider(scrapy.Spider):
    def __init__(self, asin=None, *args, **kwargs):
        super(WalmartpdpSpider, self).__init__(*args, **kwargs)
        self.product_id =asin

    name = "walmartpdp"
    start_urls = ["https://walmart.com"]

    def start_requests(self):
        url = self.product_id
        # url_dict = self.generate_url()
        yield scrapy.Request(client.scrapyGet(url=url, render=True, country_code='US'), callback=self.parse)

    def parse(self, response):
        script_tag = response.xpath('//script[@id="__NEXT_DATA__"]')
        json_data = script_tag.extract_first()
        new_data = json_data.replace('<script id="__NEXT_DATA__" type="application/json"', "")
        new_new_data = re.sub(r'nonce="[^"]*">', '', new_data)
        raw_data = new_new_data.replace("</script>","")
        data = json.loads(raw_data)
        props = data.get('props', {})
        pageProps = props.get('pageProps', {})
        initialData = pageProps.get('initialData', {})
        title = initialData.get('data').get('product').get('name')
        rating = response.css('[data-testid="reviews-and-ratings"] span.w_iUH7::text').get()
        image_count = len(initialData.get('data').get('product').get('imageInfo').get('allImages'))
        description = f"{initialData.get('data').get('idml').get('longDescription')} \n {initialData.get('data').get('idml').get('shortDescription')}"
        brand = initialData.get('data').get('seoItemMetaData').get('brand')
        is_variation = True if initialData.get('data').get('product').get('variantProductIdMap') else False
        price = initialData.get('data').get('product').get('priceInfo').get('currentPrice').get('priceString')
        product_id= self.product_id
        product_url = f"https://walmart.com{initialData.get('data').get('product').get('canonicalUrl')}"

        item_obj = WalmartPDPItem()
        item_obj['title'] = json.dumps(title),
        item_obj['rating'] = json.dumps(rating),
        item_obj['image_count'] = json.dumps(image_count),
        item_obj['description'] = json.dumps(description),
        item_obj['brand'] = json.dumps(brand),
        item_obj['is_variation'] = json.dumps(is_variation),
        item_obj['price'] = json.dumps(price),
        item_obj['product_id'] = json.dumps(product_id),
        item_obj['product_url'] = json.dumps(product_url),

        yield item_obj
