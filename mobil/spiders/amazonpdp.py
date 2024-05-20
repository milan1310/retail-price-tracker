import scrapy
from ..items import AmazonPDPItem
from scraper_api import ScraperAPIClient
client = ScraperAPIClient('')


class AmazonpdpSpider(scrapy.Spider):
    def __init__(self, asin=None, *args, **kwargs):
        super(AmazonpdpSpider, self).__init__(*args, **kwargs)
        self.asin =asin

    name = "amazonpdp"
    # allowed_domains = ["amazon.com"]

    def start_requests(self):
        yield scrapy.Request(client.scrapyGet(f"https://www.amazon.com/dp/{self.asin}/ref=cm_cr_arp_d_product_top?ie=UTF8&th=1"))
    def parse(self, response):
        item_obj = AmazonPDPItem()

        title = response.css("#productTitle::text").get()
        total_rating = response.css("#acrCustomerReviewText::text").get()
        num_images = len(response.css('li.item span.a-button-text img::attr(src)').extract())
        bulletings = response.css("#feature-bullets .a-list-item::text").getall()

        brand_element = response.css(".po-brand .a-span9 > span::text")
        brand = brand_element.get() if brand_element else "No brand mentioned"

        variation_element = response.css("#twisterContainer")
        is_variation = True if len(variation_element) > 0 else False
        is_aplus = True if response.css('.aplus-standard') else False

        buy_box_winner = response.css(".tabular-buybox-text:nth-child(6) .a-spacing-none > span > a::text").get()
        price = response.css('.a-price .a-price-whole::text').get() + '.' + response.css('.a-price .a-price-fraction::text').get()

        product_description = response.css("#productDescription::text").get()

        item_obj['title'] = title
        item_obj['total_rating'] = total_rating
        item_obj['bulletings'] = bulletings
        item_obj['is_aplus'] = is_aplus
        item_obj['buy_box_winner'] = buy_box_winner
        item_obj['num_images'] = num_images
        item_obj['price'] = price
        item_obj['brand'] = brand
        item_obj['is_variation'] = is_variation
        item_obj['asin_number'] = self.asin
        item_obj['product_url'] = f"https://www.amazon.com/dp/{self.asin}/?th=1"
        item_obj['product_description'] = product_description

        yield item_obj
