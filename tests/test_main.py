import pytest_asyncio
import pytest

from datetime import datetime
from typing import List, AsyncGenerator, Generator

import aiohttp

import pandas as pd
from deltalake import DeltaTable

import scrapy
from scrapy import responsetypes

from src import components, models
from src.src_to_brz import src_to_brz

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
async def main_page_response() -> AsyncGenerator[aiohttp.ClientResponse]:
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
