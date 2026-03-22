from typing import List, Dict, Any, Iterator, AsyncGenerator
import scrapy

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from src.models import RawScrapedPage
from src.components import Store
# To run the spyder...
from scrapy.crawler import AsyncCrawlerProcess


class SiteASpider(scrapy.Spider):
    name: str = "MrSpider"

    FORMATTED_URL_STRING: str = os.getenv(
        "SITEA_FORMATTED_URL", "https://example.com/{n}"
    )
    TIMEZONE: ZoneInfo = ZoneInfo(os.getenv("ZONEINFO_KEY", "America/Santiago"))

    n = 1
    # TODO: Separate settings from code.
    custom_settings = {
        # "ITEM_PIPELINES": {
        #     "src.src_to_brz.BronzeWriterPipeline": 100,
        # },
        "LOG_FILE": "scrapy_spider.log",
        "LOG_LEVEL": "DEBUG",
        "DOWNLOAD_DELAY": 1.0,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 3.0,
        "AUTOTHROTTLE_MAX_DELAY": 30.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 3.0,
        "HTTPCACHE_ENABLED": True,
        "HTTPCACHE_EXPIRATION_SECS": 60 * 60 * 12,
        "HTTPCACHE_DIR": "httpcache",
        "HTTPCACHE_IGNORE_HTTP_CODES": [500, 502, 503, 504, 408, 403, 404, 429],
        "SCHEDULER_DEBUG": True,
    }

    async def start(self) -> AsyncGenerator[scrapy.http.Request]:
        yield scrapy.Request(
            self.FORMATTED_URL_STRING.format(n=1), dont_filter=True, callback=self.parse
        )

    def _get_offer_links(self, content_response: scrapy.http.Response) -> List[str]:
        offer_links = content_response.css(
            "div[data-tracklisting] a.d3-ad-tile__description::attr(href)"
        ).getall()
        return offer_links

    def parse(self, response: scrapy.http.Response, n: int = 1):
        self.logger.info(f"Started scraping page #{n}")
        offer_links = self._get_offer_links(response)

        if not offer_links:
            self.logger.warning(f"No offers found for {response.url}")

        for offer_link in offer_links:
            yield response.follow(offer_link, self.parse_detail)

        yield response.follow(
            self.FORMATTED_URL_STRING.format(n=n), self.parse, cb_kwargs={"n": n + 1}
        )

    def parse_detail(self, response: scrapy.http.Response) -> Iterator[Dict[str, Any]]:
        ingestion_ts = datetime.now(tz=self.TIMEZONE)  # noqa: F821

        raw_page = RawScrapedPage(
            url=response.url,
            source_system="SiteA",
            ingestion_ts=ingestion_ts,
            raw_content=response.body,
        )

        yield raw_page.model_dump()


class BronzeWriterPipeline:
    def open_spider(self):
        pass

    def close_spider(self):
        pass

    def process_item(self, item):
        pass


def run():
    process = AsyncCrawlerProcess()
    process.crawl(SiteASpider)
    process.start()  # Blocked until crawling is finished...
