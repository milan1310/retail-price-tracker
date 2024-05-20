# import scrapy
# from scraper_api import ScraperAPIClient
# import json

# client = ScraperAPIClient("")


# class OreillypdpSpider(scrapy.Spider):
#     def __init__(self, product_id=None, *args, **kwargs):
#         super(OreillypdpSpider, self).__init__(*args, **kwargs)
#         self.product_id = "835344"

#     name = "oreillypdp"
#     allowed_domains = ["oreillyauto.com"]

#     def start_requests(self):
#         headers = {
#             "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
#             "accept-language": "en-US,en;q=0.9",
#             "cache-control": "no-cache",
#             "pragma": "no-cache",
#             "priority": "u=0, i",
#             "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
#             "sec-ch-ua-mobile": "?0",
#             "sec-ch-ua-platform": '"Windows"',
#             "sec-fetch-dest": "document",
#             "sec-fetch-mode": "navigate",
#             "sec-fetch-site": "same-origin",
#             "sec-fetch-user": "?1",
#             "upgrade-insecure-requests": "1",
#             "cookie": 'trcksesh=10779342-491f-4789-ba9c-81170da1b7e0; _fbp=fb.1.1713770763865.1918224966; __wid=439425869; LPVID=A4NmEyZTcyMmQ1Y2FhOGZk; __pdst=6256151d0b144b43b2639ec500158f94; mpt_rate_comparator_50046=40.33707612305193|1716362775326; mpt_vid=171377077532791861|1776842775327; truyoConsent={}; selectedStoreId=1276; OSESSIONID="76b6047f25679659"; _gid=GA1.2.1666449356.1713948143; mpt_recording_to_buffer_50046=1|session_timeout; mpt_conditional_import_50046=1|session_timeout; mpt_tracking_active_50046=1|session_timeout; EPCRVP=MOB-1-5-30-5QT|MOB-1-0-20-5QT; JSESSIONID=9823F12EE6D25551966143411B301396; ga_session=50b3b208-cc9a-42c6-9ac0-57e2db2b9eb3; ActiveID=ZFNM-KQMS-6TWS-WRK9-3UOK-ORP1-9RB9-BSQV; cust_id="tvNnO36DK7y49kAEbBn34UfvcDu0qVPMp/rnm+eqOF4="; trcksesh=10779342-491f-4789-ba9c-81170da1b7e0; LPSID-16349016=hmWN_bUiQ32iNaFDSq3aAQ; BVImplmain_site=14810; mpt_initial_referer=https%3A%2F%2Fwww.oreillyauto.com%2F|session; NoCookie=true; forterToken=88ce389a4ac74817bc9d88a39eec34ea_1713950658704__UDF43_13ck; fs_uid=#o-1TY56E-na1#cf580548-692a-4cf0-87a7-f2e21d333c99:26446841-05e7-465e-a852-6bec975b5e97:1713948145460::12#/1745306832; _ga=GA1.2.1766473777.1713770764; _ga_TV3LS85R98=GS1.1.1713948141.4.1.1713950742.0.0.0; fs_lua=1.1713950908810; _4c_=%7B%22_4c_s_%22%3A%22ZVPBjtowFPwV5DMB27HjBKmqEJUqLlxWqx6RY7%2BQaCFOHUO6Rfx7nwMLuzSX2DPzJu%2FF4zMZamjJgimWFpIWiopCTMkbvPdkcSa%2BsfF1IguSaVBGZGlCQVaJKHSeaFXZRDNDAWhupKjIlPyJXpxJkalMMJpepsS2Hx4eLPTNrv2sUzSVMmWoa7pwE8ZulMJqqQR70iIStR%2BW%2BtnryvvhbnUjsjxXX6URQanpbtIzMc4CerJixsRMJlWP5uEvImlKcdl5Z48mbMN7F2UDlJPeviFh4dQY2A6NDfVYz%2BkDraHZ1SHCNB%2FhzscNroamtW54lHHOH%2Bi9rBAS0dK7oYdYuaq9O8CEcYGww2Miv8aK2KyHCrwfZXUIXb%2BYz4dhmDkPzX7%2Fro%2FBzYw7zHvQ3tTff387uLLZY13fhDjRk%2B5GYBi%2Bcojvu5VrT%2BB7HRrXrn%2BgYDNfjsRyB234H9noA3zCVvrQaYzC2t7Bnd720Pfoh5CkZVpymifGFDoR3GRJoQ1NpAJuS14WUKaxv4B9XT3c5CVuJi%2BwBxPAIvuCHwC7Xbfb1%2Bu%2FO7YWqqYdyQ0e9uZ4KEfC1kMX4wttDFXno%2BDncvs6zsFUlgmFiVSzazIpZptcbmligqaKZlRximkJe7LIM0Hjg4rTPahWqRKvCU1SYCkOJCDJeV4lLKuMzaDKc5AfWY53kQtZoM%2FNkuVXx8vlHw%3D%3D%22%7D',
#             "Referer": "https://www.oreillyauto.com/search?q=mobil",
#             "Referrer-Policy": "strict-origin-when-cross-origin",
#         }
#         yield scrapy.Request(
#             client.scrapyGet(
#                 f"https://www.oreillyauto.com/detail/c/1-advanced/mobil-1-advanced-full-synthetic-motor-oil-5w-30-5-quart/mob8/15305qt?q=mobil&pos=0"
#             ),
#             headers=headers,
#         )

#     def parse(self, response):
#         import pdb

#         pdb.set_trace()
#         # data = response.body
#         yield {"name": response.css(".js-ga-product-name::text").get()}


import scrapy
from scrapy_splash import SplashRequest

class OreillypdpSpider(scrapy.Spider):
    name = 'oreillypdp'
    allowed_domains = ['oreillyauto.com']
    start_urls = [
        'https://www.oreillyauto.com/detail/c/1-advanced/mobil-1-advanced-full-synthetic-motor-oil-5w-30-5-quart/mob8/15305qt?q=mobil&pos=0'
    ]

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(url, self.parse, endpoint='execute', args={
                'lua_source': self.script(),
                'headers': {
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "accept-language": "en-US,en;q=0.9",
                    "cache-control": "no-cache",
                    "pragma": "no-cache",
                    "priority": "u=0, i",
                    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "document",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-site": "same-origin",
                    "sec-fetch-user": "?1",
                    "upgrade-insecure-requests": "1",
                    "cookie": 'trcksesh=10779342-491f-4789-ba9c-81170da1b7e0; _fbp=fb.1.1713770763865.1918224966; __wid=439425869; LPVID=A4NmEyZTcyMmQ1Y2FhOGZk; __pdst=6256151d0b144b43b2639ec500158f94; mpt_rate_comparator_50046=40.33707612305193|1716362775326; mpt_vid=171377077532791861|1776842775327; truyoConsent={}; selectedStoreId=1276; OSESSIONID="76b6047f25679659"; _gid=GA1.2.1666449356.1713948143; mpt_recording_to_buffer_50046=1|session_timeout; mpt_conditional_import_50046=1|session_timeout; mpt_tracking_active_50046=1|session_timeout; EPCRVP=MOB-1-5-30-5QT|MOB-1-0-20-5QT; JSESSIONID=9823F12EE6D25551966143411B301396; ga_session=50b3b208-cc9a-42c6-9ac0-57e2db2b9eb3; ActiveID=ZFNM-KQMS-6TWS-WRK9-3UOK-ORP1-9RB9-BSQV; cust_id="tvNnO36DK7y49kAEbBn34UfvcDu0qVPMp/rnm+eqOF4="; trcksesh=10779342-491f-4789-ba9c-81170da1b7e0; LPSID-16349016=hmWN_bUiQ32iNaFDSq3aAQ; BVImplmain_site=14810; mpt_initial_referer=https%3A%2F%2Fwww.oreillyauto.com%2F|session; NoCookie=true; forterToken=88ce389a4ac74817bc9d88a39eec34ea_1713950658704__UDF43_13ck; fs_uid=#o-1TY56E-na1#cf580548-692a-4cf0-87a7-f2e21d333c99:26446841-05e7-465e-a852-6bec975b5e97:1713948145460::12#/1745306832; _ga=GA1.2.1766473777.1713770764; _ga_TV3LS85R98=GS1.1.1713948141.4.1.1713950742.0.0.0; fs_lua=1.1713950908810; _4c_=%7B%22_4c_s_%22%3A%22ZVPBjtowFPwV5DMB27HjBKmqEJUqLlxWqx6RY7%2BQaCFOHUO6Rfx7nwMLuzSX2DPzJu%2FF4zMZamjJgimWFpIWiopCTMkbvPdkcSa%2BsfF1IguSaVBGZGlCQVaJKHSeaFXZRDNDAWhupKjIlPyJXpxJkalMMJpepsS2Hx4eLPTNrv2sUzSVMmWoa7pwE8ZulMJqqQR70iIStR%2BW%2BtnryvvhbnUjsjxXX6URQanpbtIzMc4CerJixsRMJlWP5uEvImlKcdl5Z48mbMN7F2UDlJPeviFh4dQY2A6NDfVYz%2BkDraHZ1SHCNB%2FhzscNroamtW54lHHOH%2Bi9rBAS0dK7oYdYuaq9O8CEcYGww2Miv8aK2KyHCrwfZXUIXb%2BYz4dhmDkPzX7%2Fro%2FBzYw7zHvQ3tTff387uLLZY13fhDjRk%2B5GYBi%2Bcojvu5VrT%2BB7HRrXrn%2BgYDNfjsRyB234H9noA3zCVvrQaYzC2t7Bnd720Pfoh5CkZVpymifGFDoR3GRJoQ1NpAJuS14WUKaxv4B9XT3c5CVuJi%2BwBxPAIvuCHwC7Xbfb1%2Bu%2FO7YWqqYdyQ0e9uZ4KEfC1kMX4wttDFXno%2BDncvs6zsFUlgmFiVSzazIpZptcbmligqaKZlRximkJe7LIM0Hjg4rTPahWqRKvCU1SYCkOJCDJeV4lLKuMzaDKc5AfWY53kQtZoM%2FNkuVXx8vlHw%3D%3D%22%7D',
                    "Referer": "https://www.oreillyauto.com/search?q=mobil",
                    "Referrer-Policy": "strict-origin-when-cross-origin",                             
                },
            })  


    def script(self):
        return """
                    function main(splash, args)
                -- Go to the page
                assert(splash:go(args.url))
                splash:wait(2)  -- Increase wait time if necessary to ensure the page loads completely

                -- Select the store button, checking explicitly if it exists
                local select_store = splash:select('a.js-fas-open') -- Update this selector based on actual data
                if not select_store then
                    return {error = 'Select store button not found'}
                end

                -- Click the store selection button
                select_store:mouse_click()
                splash:wait(20)  -- Wait for any dynamic content, modals, or redirections

                -- Continue with other actions, ensuring each element exists before interaction
                -- Fill in the search box
                local search_input = splash:select('input#find-a-store-search')
                if not search_input then
                    return {error = 'Search input not found'}
                end

                search_input:focus()
                search_input:send_text('60007')
                splash:wait(0.5)
                search_input:send_keys('<Enter>')
                splash:wait(2)

                -- Example of how to handle subsequent steps
                -- Make sure to confirm selectors and add similar checks for each interaction
                return {
                    html = splash:html(),
                    url = splash:url(),
                }
            end
        """

    def parse(self, response):
        # Extract data or perform additional processing here    
        # self.logger.info('URL after interaction: %s', response.data['url'])
        yield {
            'html': response.data['html']
        }
