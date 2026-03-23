from pathlib import Path
from typing import List, Iterator, AsyncGenerator
import scrapy

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from src.models import RawScrapedPage
from src.components import Store, StoreConfig

from scrapy.crawler import AsyncCrawlerProcess
from scrapy.exceptions import DropItem


from dotenv import load_dotenv

load_dotenv()


class RawScrapedPageScrapyItem(scrapy.Item):
    url = scrapy.Field()
    source_system = scrapy.Field()
    ingestion_ts = scrapy.Field()
    raw_content = scrapy.Field()


class SiteASpider(scrapy.Spider):
    name: str = "MrSpider"

    FORMATTED_URL_STRING: str = os.getenv(
        "SITEA_FORMATTED_URL", "https://example.com/{n}"
    )
    TIMEZONE: ZoneInfo = ZoneInfo(os.getenv("ZONEINFO_KEY", "America/Santiago"))

    async def start(self) -> AsyncGenerator[scrapy.http.Request]:
        yield scrapy.Request(
            self.FORMATTED_URL_STRING.format(n=1), dont_filter=True, callback=self.parse
        )

    def _get_offer_links(self, content_response: scrapy.http.Response) -> List[str]:
        offer_links = content_response.css(
            "div[data-tracklisting] a.d3-ad-tile__description::attr(href)"
        ).getall()
        return offer_links

    def _keep_scraping(self) -> bool:
        if self.crawler.stats:
            return self.crawler.stats.get_value("item_dropped_count", 0) == 0
        else:
            raise ValueError("No crawler stats found...")

    def parse(self, response: scrapy.http.Response, n: int = 1):
        if self._keep_scraping():
            self.logger.info(f"Started scraping page #{n}")
            offer_links = self._get_offer_links(response)

            if not offer_links:
                self.logger.warning(f"No offers found for {response.url}")

            for offer_link in offer_links:
                yield response.follow(offer_link, self.parse_detail)

            yield response.follow(
                self.FORMATTED_URL_STRING.format(n=n),
                self.parse,
                cb_kwargs={"n": n + 1},
            )

    def parse_detail(self, response: scrapy.http.Response) -> Iterator[RawScrapedPage]:
        ingestion_ts = datetime.now(tz=self.TIMEZONE)  # noqa: F821

        raw_page = RawScrapedPage(
            url=response.url,
            source_system="SiteA",
            ingestion_ts=ingestion_ts,
            raw_content=response.body,
        )

        yield raw_page


class BronzeWriterPipeline:
    pages_in_memory: List[RawScrapedPage] = list()
    max_pages_in_memory: int = 500

    def open_spider(self):
        store_config = StoreConfig(
            Path(os.getenv("BRONZE_DELTALAKE_LOCATION", "data/bronze")),
            Path(os.getenv("SILVER_DELTALAKE_LOCATION", "data/silver")),
        )

        self.store = Store(store_config)

    async def close_spider(self):
        await self.store.write_batch_of_entries(self.pages_in_memory)

    async def process_item(self, item: RawScrapedPage):
        if await self.store.check_if_entry_exists(item):
            raise DropItem("This offer was already extracted")

        self.pages_in_memory.append(item)

        if len(self.pages_in_memory) >= self.max_pages_in_memory:
            batch_to_write = self.pages_in_memory
            self.pages_in_memory = []
            await self.store.write_batch_of_entries(batch_to_write)
            
        return item


def run():
    custom_settings = {
        "ITEM_PIPELINES": {
            "src.src_to_brz.src_to_brz.BronzeWriterPipeline": 100,
        },
        "LOG_FILE": os.getenv("SCRAPY_LOG_FILE", "scrapy_spider.log"),
        "LOG_LEVEL": os.getenv("SCRAPY_LOG_LEVEL", "DEBUG"),
        "DOWNLOAD_DELAY": float(os.getenv("SCRAPY_DOWNLOAD_DELAY", "1.0")),
        "AUTOTHROTTLE_ENABLED": os.getenv("SCRAPY_AUTOTHROTTLE_ENABLED", "True").lower()
        == "true",
        "AUTOTHROTTLE_START_DELAY": float(
            os.getenv("SCRAPY_AUTOTHROTTLE_START_DELAY", "2.0")
        ),
        "AUTOTHROTTLE_MAX_DELAY": float(
            os.getenv("SCRAPY_AUTOTHROTTLE_MAX_DELAY", "20.0")
        ),
        "AUTOTHROTTLE_TARGET_CONCURRENCY": float(
            os.getenv("SCRAPY_AUTOTHROTTLE_TARGET_CONCURRENCY", "15.0")
        ),
        "HTTPCACHE_ENABLED": os.getenv("SCRAPY_HTTPCACHE_ENABLED", "True").lower()
        == "true",
        "HTTPCACHE_EXPIRATION_SECS": int(
            os.getenv("SCRAPY_HTTPCACHE_EXPIRATION_SECS", str(60 * 60 * 12))
        ),
        "HTTPCACHE_DIR": os.getenv("SCRAPY_HTTPCACHE_DIR", "httpcache"),
        "HTTPCACHE_IGNORE_HTTP_CODES": [500, 502, 503, 504, 408, 403, 404, 429],
        "SCHEDULER_DEBUG": os.getenv("SCRAPY_SCHEDULER_DEBUG", "True").lower()
        == "true",
    }

    process = AsyncCrawlerProcess(settings=custom_settings)
    process.crawl(SiteASpider)
    process.start()  # Blocked until crawling is finished...


if __name__ == "__main__":
    run()
