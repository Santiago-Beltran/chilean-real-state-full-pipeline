from src.models import RawScrapedPage
import pytest_asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

from datetime import datetime
from typing import List, AsyncGenerator, Generator

import aiohttp
import asyncio

import pandas as pd
from deltalake import DeltaTable

import scrapy
from scrapy import responsetypes

from src import components, models
from src.src_to_brz import src_to_brz
from src.src_to_brz.src_to_brz import BronzeWriterPipeline

from pathlib import Path

import shutil
import os

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent
RESOURCES = BASE_DIR / "resources"

# Both folders will be deleted as per _clean_test_store_folders. 
TEST_DELTALAKE_BRONZE = Path(RESOURCES / "test_deltalake/bronze")
TEST_DELTALAKE_SILVER = Path(RESOURCES / "test_deltalake/silver")


@pytest_asyncio.fixture
async def main_page_response() -> AsyncGenerator[aiohttp.ClientResponse]: # Live requests are unstable and unsuitable for testing. I should instead create a monitoring service that checks whether the scraper works or not.
    async with aiohttp.ClientSession() as session:
        async with session.get(os.getenv("SITEA_START_URL", "Example.com")) as response:
            response.raise_for_status()
            yield response


@pytest.fixture
def raw_scraped_page() -> models.RawScrapedPage:
    return models.RawScrapedPage(
        url="Example.com",
        source_system="Test",
        ingestion_ts=datetime.now(),
        raw_content=b"Test",
    )


def _clean_test_store_folders():
    shutil.rmtree(TEST_DELTALAKE_BRONZE, ignore_errors=True)
    shutil.rmtree(TEST_DELTALAKE_SILVER, ignore_errors=True)


@pytest.fixture
def store() -> Generator[components.Store]:

    _clean_test_store_folders()

    test_config = components.StoreConfig(TEST_DELTALAKE_BRONZE, TEST_DELTALAKE_SILVER)
    yield components.Store(test_config)

    _clean_test_store_folders()


@pytest.mark.asyncio
async def test_select_all_offers_on_page(main_page_response: aiohttp.ClientResponse):
    content = await main_page_response.content.read()
    test_url: str = "example.com"

    respcls = responsetypes.responsetypes.from_args(
        headers=None, url=test_url, body=content
    )

    response: scrapy.http.Response = respcls(url=test_url, body=content)

    test_spider = src_to_brz.SiteASpider("test_spider")

    offer_links: List[str] = test_spider._get_offer_links(response)

    for offer_link in offer_links:
        assert offer_link.startswith("/bienes-raices")

    assert offer_links


@pytest.mark.asyncio
async def test_raw_to_brz_write(
    raw_scraped_page: models.RawScrapedPage, store: components.Store
):
    await store.write_batch_of_entries([raw_scraped_page])

    df: pd.DataFrame = DeltaTable(TEST_DELTALAKE_BRONZE / "bronze_metadata").to_pandas()

    assert len(df) == 1


@pytest.mark.asyncio
async def test_brz_check_if_file_exists(
    raw_scraped_page: models.RawScrapedPage, store: components.Store
):
    assert not await store.check_if_entry_exists(raw_scraped_page)

    await store.write_batch_of_entries([raw_scraped_page])

    assert await store.check_if_entry_exists(raw_scraped_page)

@pytest.mark.asyncio
async def test_too_many_open_files(
        raw_scraped_page: models.RawScrapedPage, store: components.Store
        ):

    many_files: List[RawScrapedPage] = [raw_scraped_page.model_copy() for _ in range(500)]

    await store.write_batch_of_entries(many_files)

    assert True


@pytest.mark.asyncio
async def test_pipeline_race_condition_fix():
    pipeline = BronzeWriterPipeline()
    pipeline.max_pages_in_memory = 10
    pipeline.pages_in_memory = []  
    
    pipeline.store = MagicMock()
    pipeline.store.check_if_entry_exists = AsyncMock(return_value=False)
    
    write_mock = AsyncMock()
    async def slow_write(*args, **kwargs):
        await asyncio.sleep(0.1)
    
    write_mock.side_effect = slow_write
    pipeline.store.write_batch_of_entries = write_mock

    items = []
    for i in range(10):
        items.append(
            RawScrapedPage(
                url=f"https://example.com/{i}",
                source_system="SiteA",
                ingestion_ts=datetime.now(),
                raw_content=b"<html>test</html>"
            )
        )

    tasks = [pipeline.process_item(item) for item in items]
    await asyncio.gather(*tasks)

    assert pipeline.store.write_batch_of_entries.call_count == 1, "Write was called multiple times."
    
    written_batch = pipeline.store.write_batch_of_entries.call_args[0][0]
    assert len(written_batch) == 10
    
    assert len(pipeline.pages_in_memory) == 0
