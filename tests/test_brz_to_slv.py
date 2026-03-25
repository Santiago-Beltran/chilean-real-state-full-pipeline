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
def test_siteA_get_offer_id(page_data: siteAPageWithAnswers):
    assert (
        SiteAParser._get_offer_id(page_data.url)
        == page_data.expected_silver_entry.offer_id
    )


@pytest.mark.parametrize("page_data", PAGE_TEST_CASES, ids=custom_test_id)
def test_siteA_get_offer_date(page_data: siteAPageWithAnswers):
    soup = BeautifulSoup(page_data.content, "html.parser")
    details_dict = SiteAParser._get_details_dict(soup)

    assert (
        SiteAParser._get_offer_date(soup, details_dict)
        == page_data.expected_silver_entry.offer_date
    )


@pytest.mark.parametrize("page_data", PAGE_TEST_CASES, ids=custom_test_id)
def test_siteA_get_offer_type(page_data: siteAPageWithAnswers):
    soup = BeautifulSoup(page_data.content, "html.parser")
    loopa_data = SiteAParser._get_loopa_data_dict(soup)

    assert (
        SiteAParser._get_offer_type(loopa_data)
        == page_data.expected_silver_entry.offer_type
    )


@pytest.mark.parametrize("page_data", PAGE_TEST_CASES, ids=custom_test_id)
def test_siteA_get_property_type(page_data: siteAPageWithAnswers):

    assert (
        SiteAParser._get_property_type(page_data.url)
        == page_data.expected_silver_entry.property_type
    )


@pytest.mark.parametrize("page_data", PAGE_TEST_CASES, ids=custom_test_id)
def test_siteA_get_property_listed_price(page_data: siteAPageWithAnswers):

    soup = BeautifulSoup(page_data.content, "html.parser")

    assert SiteAParser._get_offer_price(soup) == page_data.expected_silver_entry.price


@pytest.mark.parametrize("page_data", PAGE_TEST_CASES, ids=custom_test_id)
def test_siteA_get_total_sqm(page_data: siteAPageWithAnswers):

    soup = BeautifulSoup(page_data.content, "html.parser")

    insights_dict = SiteAParser._get_insights_dict(soup) # I dislike relying on the implementation of these functions that aren't being tested in the test. Perhaps this could be some internal component of the tested functions, this has some cons such as re-computing the same insights_dict on each method. 
    details_dict = SiteAParser._get_details_dict(soup)

    assert (
        SiteAParser._get_total_sqm(insights_dict, details_dict)
        == page_data.expected_silver_entry.total_sqm
    )


@pytest.mark.parametrize("page_data", PAGE_TEST_CASES, ids=custom_test_id)
def test_siteA_get_built_sqm(page_data: siteAPageWithAnswers):
    soup = BeautifulSoup(page_data.content, "html.parser")

    insights_dict = SiteAParser._get_insights_dict(soup)
    details_dict = SiteAParser._get_details_dict(soup)
    assert (
        SiteAParser._get_built_sqm(insights_dict, details_dict)
        == page_data.expected_silver_entry.built_sqm
    )


@pytest.mark.parametrize("page_data", PAGE_TEST_CASES, ids=custom_test_id)
def test_siteA_get_chilean_region_name(page_data: siteAPageWithAnswers):
    soup = BeautifulSoup(page_data.content, "html.parser")

    data_layer = SiteAParser._get_data_layer_dict(soup)
    loopa_data = SiteAParser._get_loopa_data_dict(soup)
    assert (
        SiteAParser._get_chilean_region_name(data_layer, loopa_data)
        == page_data.expected_silver_entry.chilean_region_name
    )


@pytest.mark.parametrize("page_data", PAGE_TEST_CASES, ids=custom_test_id)
def test_siteA_get_chilean_location_name(page_data: siteAPageWithAnswers):
    soup = BeautifulSoup(page_data.content, "html.parser")

    data_layer = SiteAParser._get_data_layer_dict(soup)
    loopa_data = SiteAParser._get_loopa_data_dict(soup)

    assert (
        SiteAParser._get_chilean_location_name(data_layer, loopa_data)
        == page_data.expected_silver_entry.chilean_location_name
    )


@pytest.mark.parametrize("page_data", PAGE_TEST_CASES, ids=custom_test_id)
def test_siteA_get_number_of_rooms(page_data: siteAPageWithAnswers):
    soup = BeautifulSoup(page_data.content, "html.parser")

    insights_dict = SiteAParser._get_insights_dict(soup)
    details_dict = SiteAParser._get_details_dict(soup)
    loopa_data = SiteAParser._get_loopa_data_dict(soup)

    assert (
        SiteAParser._get_number_of_rooms(insights_dict, details_dict, loopa_data)
        == page_data.expected_silver_entry.number_of_rooms
    )


@pytest.mark.parametrize("page_data", PAGE_TEST_CASES, ids=custom_test_id)
def test_siteA_get_number_of_bathrooms(page_data: siteAPageWithAnswers):
    soup = BeautifulSoup(page_data.content, "html.parser")

    insights_dict = SiteAParser._get_insights_dict(soup)
    details_dict = SiteAParser._get_details_dict(soup)

    assert (
        SiteAParser._get_number_of_bathrooms(insights_dict, details_dict)
        == page_data.expected_silver_entry.number_of_bathrooms
    )


@pytest.mark.parametrize("page_data", PAGE_TEST_CASES, ids=custom_test_id)
def test_siteA_get_number_of_parking_spots(page_data: siteAPageWithAnswers):
    soup = BeautifulSoup(page_data.content, "html.parser")

    insights_dict = SiteAParser._get_insights_dict(soup)
    details_dict = SiteAParser._get_details_dict(soup)

    assert (
        SiteAParser._get_number_of_parking_spots(insights_dict, details_dict)
        == page_data.expected_silver_entry.number_of_parking_spots
    )


@pytest.mark.parametrize("page_data", PAGE_TEST_CASES, ids=custom_test_id)
def test_siteA_get_latitude_and_longitude(page_data: siteAPageWithAnswers):
    soup = BeautifulSoup(page_data.content, "html.parser")

    lat, lon = SiteAParser._get_lat_lon(soup)

    assert lat == page_data.expected_silver_entry.latitude
    assert lon == page_data.expected_silver_entry.longitude
