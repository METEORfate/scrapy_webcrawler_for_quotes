import scrapy
from scrapy import Request
from ..items import QuoteItem

class QuotesspiderSpider(scrapy.Spider):
    name = "QuotesSpider"
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["https://quotes.toscrape.com"]
    headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    def parse(self, response):
        quotes = response.xpath('//div[@class="quote"]')
        for quote in quotes:
            item = QuoteItem()
            item['text'] = quote.xpath('.//span[@class="text"]/text()').get()
            item['author'] = quote.xpath('.//small[@class="author"]/text()').get()
            item['tags'] = quote.xpath('.//div[@class="tags"]/a[@class="tag"]/text()').getall()
            yield item
        
        # 处理分页
        next_page = response.xpath('//li[@class="next"]/a/@href').get()
        if next_page:
            yield Request(f"https://quotes.toscrape.com{next_page}",headers=self.headers,callback=self.parse)
