import scrapy
from scraper_api import ScraperAPIClient
import urllib.parse
import json

client = ScraperAPIClient("")
ScraperAPI = True


class Walamart3pSpider(scrapy.Spider):
    def __init__(self, asin=None, *args, **kwargs):
        super(Walamart3pSpider, self).__init__(*args, **kwargs)
        self.product_id =asin
    name = "walmart3p"
    allowed_domains = ["walmart.com"]
    handle_httpstatus_list = [500, 404]

    def generate_url(self, product_id):
        urls = []
        payload = {"itemId": product_id, "isSubscriptionEligible": True}
        encoded_payload = urllib.parse.quote_plus(json.dumps(payload))

        headers = {
            "accept": "application/json",
            "accept-language": "en-US",
            "content-type": "application/json",
            "device_profile_ref_id": "1ccDoaxBU5EXnKws3GC3fcjenSTvhcGeCdeO",
            "downlink": "10",
            "dpr": "1",
            "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "tenant-id": "elh9ie",
            "traceparent": "00-48fa03bdb36c0517320dcb1d71245637-c7d81fe6d343fe35-00",
            "wm_mp": "true",
            "wm_page_url": f"https://www.walmart.com/ip/Mobil-1-High-Mileage-Full-Synthetic-Motor-Oil-5W-20-5-Quart/{product_id}?athbdg=null%26athbdg%3DL1102_L1102&from=/search",
            "wm_qos.correlation_id": "bs0ZRlIAMcapZK6Yza_keTvJHhwwyN6qppUX",
            "x-apollo-operation-name": "GetAllSellerOffers",
            "x-enable-server-timing": "1",
            "x-latency-trace": "1",
            "x-o-bu": "WALMART-US",
            "x-o-ccm": "server",
            "x-o-correlation-id": "bs0ZRlIAMcapZK6Yza_keTvJHhwwyN6qppUX",
            "x-o-gql-query": "query GetAllSellerOffers",
            "x-o-mart": "B2C",
            "x-o-platform": "rweb",
            "x-o-platform-version": "us-web-1.131.0-804db680b11f6c3f50de81a358b2611151dd95d8-041813",
            "x-o-segment": "oaoh",
            "cookie": "ACID=322f702c-d574-4e00-a96b-e11c5ea1fdd2; hasACID=true; _m=9; ak_bmsc=A98E89105B760426A89DA0BB2DE100AC~000000000000000000000000000000~YAAQVQHVFzWzE/yOAQAAlt0AChd0239/z5LxhTR8ZtRV5AntdUkza3/xHGYOdakK/GI/PiBSg2Cj8R/X1EqZGX+jFi42C01nHxqwgESTbK6lbwMomGMJffhC1OKmq328X67QrNTUg9iCvMXtBFVjY+228C+aS6Sv+TWHg43/WPpxnCLwypF7gRFT3F5F/jBeBsdbZH3zCFWjCoqUhwIOrkFnbN6llJIe9fMxTnEtmgn5jW7em2EoZUXsAQNp3wm35v4GTynWHDeFSDfWnn5pUXC6vCaBquwLlL6pW9rH3i14mtArnCISZYfGCsVfYklSb4J5PXOTqtke86Jcq27WC4mP3Lxq2Fr0WmoDnMZM4G/FVita5yMCW5ccO8RaXzyXiVKQ34ftpEYS/p6WBw==; TBV=7; _astc=ebae75372e5161706cb2b2739f09c812; adblocked=false; auth=MTAyOTYyMDE4CvjogzmIjz0ox4dHSmVBGnLrLSdeRHOfKk5sXwnz%2F%2F%2FCZppyNjSfr8f%2FRVZZxE28pILU5LSo5O41O2q49iVJJ1ptq3r3vDT7Mi7s1Ie6SEYiS8VPH88D6Au%2BFaIiUVCK767wuZloTfhm7Wk2KcjygsAEeU%2BeKCMhfP9XV060SY8kRyeAtdMOS9Zu%2BW%2BwMYetUwJKbz8WUNzui8nthZ%2BS5ws8S4%2F%2FOyzrcGKK%2F4dNN%2BgUMk70P8glgOEpLOprhDfMJ0tmvH1FCaN9tZDh4SCrHd4h6u2i8jcUPoDfrU%2FksEAogDymwHZT8%2BduXIUh2aMSIKvJvS6h4zf%2BfFhp4MqpHVP8b8VPjfO8Bqu3R047pnN75BGbX%2FoTvFKK3al1%2BncN%2BOeEcQ79eho%2F4VdqDyiDOUjyrOXbKKhH072NS%2FW0j%2FU%3D; locDataV3=eyJpc0RlZmF1bHRlZCI6dHJ1ZSwiaXNFeHBsaWNpdCI6ZmFsc2UsImludGVudCI6IlNISVBQSU5HIiwicGlja3VwIjpbeyJidUlkIjoiMCIsIm5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJub2RlVHlwZSI6IlNUT1JFIiwiYWRkcmVzcyI6eyJwb3N0YWxDb2RlIjoiOTU4MjkiLCJhZGRyZXNzTGluZTEiOiI4OTE1IEdFUkJFUiBST0FEIiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeSI6IlVTIiwicG9zdGFsQ29kZTkiOiI5NTgyOS0wMDAwIn0sImdlb1BvaW50Ijp7ImxhdGl0dWRlIjozOC40ODI2NzcsImxvbmdpdHVkZSI6LTEyMS4zNjkwMjZ9LCJpc0dsYXNzRW5hYmxlZCI6dHJ1ZSwic2NoZWR1bGVkRW5hYmxlZCI6dHJ1ZSwidW5TY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJodWJOb2RlSWQiOiIzMDgxIiwic3RvcmVIcnMiOiIwNjowMC0yMzowMCIsInN1cHBvcnRlZEFjY2Vzc1R5cGVzIjpbIlBJQ0tVUF9TUEVDSUFMX0VWRU5UIiwiUElDS1VQX0lOU1RPUkUiLCJQSUNLVVBfQ1VSQlNJREUiXSwic2VsZWN0aW9uVHlwZSI6IkRFRkFVTFRFRCJ9XSwic2hpcHBpbmdBZGRyZXNzIjp7ImxhdGl0dWRlIjozOC40NzQ2LCJsb25naXR1ZGUiOi0xMjEuMzQzOCwicG9zdGFsQ29kZSI6Ijk1ODI5IiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeUNvZGUiOiJVU0EiLCJnaWZ0QWRkcmVzcyI6ZmFsc2UsInRpbWVab25lIjoiQW1lcmljYS9Mb3NfQW5nZWxlcyJ9LCJhc3NvcnRtZW50Ijp7Im5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJpbnRlbnQiOiJQSUNLVVAifSwiaW5zdG9yZSI6ZmFsc2UsImRlbGl2ZXJ5Ijp7ImJ1SWQiOiIwIiwibm9kZUlkIjoiMzA4MSIsImRpc3BsYXlOYW1lIjoiU2FjcmFtZW50byBTdXBlcmNlbnRlciIsIm5vZGVUeXBlIjoiU1RPUkUiLCJhZGRyZXNzIjp7InBvc3RhbENvZGUiOiI5NTgyOSIsImFkZHJlc3NMaW5lMSI6Ijg5MTUgR0VSQkVSIFJPQUQiLCJjaXR5IjoiU2FjcmFtZW50byIsInN0YXRlIjoiQ0EiLCJjb3VudHJ5IjoiVVMiLCJwb3N0YWxDb2RlOSI6Ijk1ODI5LTAwMDAifSwiZ2VvUG9pbnQiOnsibGF0aXR1ZGUiOjM4LjQ4MjY3NywibG9uZ2l0dWRlIjotMTIxLjM2OTAyNn0sImlzR2xhc3NFbmFibGVkIjp0cnVlLCJzY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJ1blNjaGVkdWxlZEVuYWJsZWQiOnRydWUsImFjY2Vzc1BvaW50cyI6W3siYWNjZXNzVHlwZSI6IkRFTElWRVJZX0FERFJFU1MifV0sImh1Yk5vZGVJZCI6IjMwODEiLCJpc0V4cHJlc3NEZWxpdmVyeU9ubHkiOmZhbHNlLCJzdXBwb3J0ZWRBY2Nlc3NUeXBlcyI6WyJERUxJVkVSWV9BRERSRVNTIl0sInNlbGVjdGlvblR5cGUiOiJMU19TRUxFQ1RFRCJ9LCJyZWZyZXNoQXQiOjE3MTM4NjMzODA2NTEsInZhbGlkYXRlS2V5IjoicHJvZDp2MjozMjJmNzAyYy1kNTc0LTRlMDAtYTk2Yi1lMTFjNWVhMWZkZDIifQ%3D%3D; assortmentStoreId=3081; hasLocData=1; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsImlzRXhwbGljaXQiOmZhbHNlLCJzdG9yZUludGVudCI6IlBJQ0tVUCIsIm1lcmdlRmxhZyI6ZmFsc2UsImlzRGVmYXVsdGVkIjp0cnVlLCJwaWNrdXAiOnsibm9kZUlkIjoiMzA4MSIsInRpbWVzdGFtcCI6MTcwNjg3NzE5NjEyNCwic2VsZWN0aW9uVHlwZSI6IkRFRkFVTFRFRCJ9LCJzaGlwcGluZ0FkZHJlc3MiOnsidGltZXN0YW1wIjoxNzA2ODc3MTk2MTI0LCJ0eXBlIjoicGFydGlhbC1sb2NhdGlvbiIsImdpZnRBZGRyZXNzIjpmYWxzZSwicG9zdGFsQ29kZSI6Ijk1ODI5IiwiZGVsaXZlcnlTdG9yZUxpc3QiOlt7Im5vZGVJZCI6IjMwODEiLCJ0eXBlIjoiREVMSVZFUlkiLCJ0aW1lc3RhbXAiOjE3MTM4NTk3ODA2NDcsImRlbGl2ZXJ5VGllciI6bnVsbCwic2VsZWN0aW9uVHlwZSI6IkxTX1NFTEVDVEVEIiwic2VsZWN0aW9uU291cmNlIjpudWxsfV0sImNpdHkiOiJTYWNyYW1lbnRvIiwic3RhdGUiOiJDQSJ9LCJwb3N0YWxDb2RlIjp7InRpbWVzdGFtcCI6MTcwNjg3NzE5NjEyNCwiYmFzZSI6Ijk1ODI5In0sIm1wIjpbXSwidmFsaWRhdGVLZXkiOiJwcm9kOnYyOjMyMmY3MDJjLWQ1NzQtNGUwMC1hOTZiLWUxMWM1ZWExZmRkMiJ9; abqme=true; vtc=dyHFzzYd9Z2zFFvE8xvIAc; bstc=dyHFzzYd9Z2zFFvE8xvIAc; mobileweb=0; xpth=x-o-mart%2BB2C~x-o-mverified%2Bfalse; xpa=07Vx8|0KCJN|1cZ20|2olBl|A4z0Q|A_XEa|CLmPX|CsQik|H5b4F|HeSSx|I1d3e|IWB6I|LDI19|LRUw2|P4Rfd|QajCC|TEMOf|Tiu_i|TpjqX|UGqpb|ZWsQm|Zy9Hv|_VmSf|fdm-7|fpYqh|he945|nVDKp|oBS8o|ox_Di|uFi2G|x1ajX; exp-ck=07Vx830KCJN11cZ201CLmPX2H5b4F1HeSSx3I1d3e1IWB6I1LDI193LRUw21P4Rfd3TEMOf1Tiu_i1Zy9Hv1fdm-71fpYqh2he9451; _pxhd=a54237fffc42fe792829c84edfbdea0c37fbab0b4cc77fe6d0ac7a7e21047c5a:d5a650ee-0148-11ef-94a6-6f71bb06f618; bm_mi=6805ADA2581CA43D8EBD5EAD79A8310D~YAAQVQHVF0uzE/yOAQAA6uAAChdri6NMIp8JHK0AKpAKC93oGMQI7ULOd5LT8sH7P7hUWt6CP54lQEPVqDN/hnw19vKVJz/bdH9QCLzTb1Nfq9Bcq/hh62noAxrT/18GB+QyboCg11dEAOS6uj6s5I/WDpx/6WbsxVCIuI/wjge0N1LPUh5Gl3fh7Of19yZklCGA1jnLSm3ExkUDZ2x9nLXLlz9dAD8IXeCDIi3kww2GBHscOoncL5Gm5lUMeXMeaGMTb8mvouxmMGkVAVu3dSZL2upbVruaqAcpvNcWFG6P2IO8C71WDQS/gTuqgbg=~1; xptc=assortmentStoreId%2B3081; xpm=1%2B1713859781%2BdyHFzzYd9Z2zFFvE8xvIAc~%2B0; xptwj=qq:6aa90fd07f9835f06688:G0tZWISYRUeekcqcwsSpb5oJ7VGjdAoCWYnhHgRTFznjizLMLAyN4N1mSOoaySB475GjzTXGrcptj5LVeK//lm5Pn33qK+Nf2xGcCpftKaNokcbn/dwbmrvThVdGYaVy6NnDHXRoTQguZHrtF9APOD+RtNVZmw==; xptwg=1860949313:18D9D8C1866C120:3E46EAA:646113D:3228AEDB:4FC95715:; TS012768cf=016ea84bd23f59d036abdc7acd9a8a55045562597ac97eea2cf2f81d1b0fad040c12147ca8de7f69ccc2d73acc63cce4173f3ae178; TS01a90220=016ea84bd23f59d036abdc7acd9a8a55045562597ac97eea2cf2f81d1b0fad040c12147ca8de7f69ccc2d73acc63cce4173f3ae178; TS2a5e0c5c027=08039d86dcab20001d8f444ea2443cf9b952a24eae8274b65c544b41e35af66e4e33c5950d018bee088f245cf7113000c1d9bfb10615244c611ae5b8b49e13836d8e1e50fb2180a175f000c5fa0b14a38d28aac14637405e35146cb9385c482d; akavpau_p2=1713860390~id=5e4670992cabf6776a7c0a88c2b8934f; bm_sv=E99A69AF20854EF61DAD4C46988E89A5~YAAQVQHVFxW0E/yOAQAAjAgBChcGZQoKateaYzcChwp0y1oDyF1ilWLGL94UWp0ZzidJbUMhy6aK/9qi6gtkthq/cDDcR/tM1XTGpSxT9YUK7zcLn1SGtl5Ofpo2Z90p95PYM01OLKR8uUerjd/pHeKBSfdNeH4SOK4GBu2Qoa15/IAL4eCq35Bv/KwZDuRUZ4ONsZvYj6eF6YeWC1GGOKGZcvyOsjGbFn3uL9gMtrqqnGE95rNAUAQVvyjUlE4NMA==~1; pxcts=dc477995-0148-11ef-b023-dd17fe1cfb78; _pxvid=d5a650ee-0148-11ef-94a6-6f71bb06f618; _px3=cc8041ab1caa4872e92fcdbf45f895d522593d2c6dc7ff5368a6e4a0e5eecde7:1Cx5Nb+E12S+YzVEFRKb66VpWhdi+lvXmL3rf5E3elzvBTjEa1nbkI+8SdGA0cjNYTJYK4bhNpXpRoMWXKl5+A==:1000:AZ7gNWVENEzeUPyq6n0TusE9WJ8tOgCE4r+GuZXEB4a0MPW3hWcMKIq0rCM0GvvZ5UNm10W2s1wnaKFst8DUP+clM7J+20h3selNxdhsOmhmcBTvP90H0zqoxyMGLIIu8jyeGvOQ3MAjHyFaUO2lKnNB50Ek0ciLs2woR0mSEAz8tgB3lxDgSPXUm+uQKAONUR7IfP5KnZl/vShEGre0zAA3IPeqyt1Dzy4PjjwCZMU=; _pxde=cd4d51abb6cf04327288a65da529d279b810826acfee613b255561ba74d514e3:eyJ0aW1lc3RhbXAiOjE3MTM4NTk3OTkyNTF9",
            "Referer": f"https://www.walmart.com/ip/Mobil-1-High-Mileage-Full-Synthetic-Motor-Oil-5W-20-5-Quart/{product_id}?athbdg=null%26athbdg%3DL1102_L1102&from=/search",
            "Referrer-Policy": "strict-origin-when-cross-origin",
        }

        url = f"https://www.walmart.com/orchestra/home/graphql/GetAllSellerOffers/da06c4b54698f8778cede1971c1bb9826934be690b8fbc0f3d02e23e538e2d7e?variables={encoded_payload}"

        urls.append({"url": url, "headers": headers})
        return urls

    def start_requests(self):
        for url_dict in self.generate_url(product_id=self.product_id):
            yield scrapy.Request(client.scrapyGet(url_dict['url']),headers=url_dict['headers'], callback=self.parse)

    def parse(self, response):
        if response.status == 500:
            yield{
                'asin': self.product_id,
                'title': 'NA',
                'price': 'NA',
                'shipped_from':'NA',
                'sold_by': 'NA',
                'seller_store_url':'NA'
            }
        else:
            data_dict = json.loads(response.body)
            for data in data_dict.get('data').get('product').get('allOffers'):
                yield{
                    'asin': self.product_id,
                    'title': 'new',
                    'price': data.get('priceInfo').get('currentPrice').get('price'),
                    'shipped_from':data.get('sellerName'),
                    'sold_by': data.get('sellerName'),
                    'seller_store_url':data.get('sellerStoreFrontURL', None)
                }