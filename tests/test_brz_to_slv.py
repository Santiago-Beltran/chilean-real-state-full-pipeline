from typing import List
from src.models import SilverStorageEntry
import pytest

from pathlib import Path
import os

import json
from dataclasses import dataclass

from src.components import SiteAParser

from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).parent
RESOURCES = BASE_DIR / "resources"


@dataclass
class siteAPageWithAnswers:
    url: str
    content: str
    expected_silver_entry: SilverStorageEntry


def create_siteA_pages_with_answers() -> List[siteAPageWithAnswers]:
    """
    Constructs siteAPageWithAnswers based off of tests/resources/detailed_offers/pages{n} given a determined naming convention in folders.

    See tests/resources/detailed_offers/page1 for an example.
    """

    created_list: List[siteAPageWithAnswers] = list()

    detailed_offers_path = RESOURCES / "detailed_offers/"

    for item in os.listdir(detailed_offers_path):
        folder_path = Path(os.path.join(detailed_offers_path, item))
        if item.find("page") == -1:
            continue

        if os.path.isdir(folder_path):
            with open(folder_path / "page_content.html") as c:
                content = c.read()

            with open(folder_path / "page_url.txt") as u:
                url = u.read().strip()

            with open(folder_path / "test_answers.json") as a:
                data = json.load(a)

            expected_silver_entry = SilverStorageEntry(**data)

            created_list.append(
                siteAPageWithAnswers(url, content, expected_silver_entry)
            )

    return created_list

def custom_test_id(page_data: siteAPageWithAnswers):
    return page_data.url


PAGE_TEST_CASES = create_siteA_pages_with_answers()

@pytest.mark.parametrize("page_data", PAGE_TEST_CASES, ids=custom_test_id)
def test_siteA_page_extraction(page_data: siteAPageWithAnswers):
    assert SiteAParser._get_offer_id(page_data.url) == page_data.expected_silver_entry.offer_id
