from bs4 import BeautifulSoup
from abc import abstractmethod
from deltalake import write_deltalake, DeltaTable
import asyncio
import aiofiles
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from functools import singledispatchmethod

import pandas as pd
import pyarrow as pa

from src.models import RawScrapedPage, BronzeStorageEntry, SilverStorageEntry

import os
import re


class StoreConfig:
    def __init__(self, bronze_path: Path, silver_path: Path):
        self.bronze_path = bronze_path
        self.silver_path = silver_path

        self.bronze_metadata_path = bronze_path / "bronze_metadata"


class Store:
    def __init__(self, config: StoreConfig):
        self.config = config

        self._ensure_folders()

    def _ensure_folders(self):
        if self.config.bronze_path:
            os.makedirs(self.config.bronze_path, exist_ok=True)

        if self.config.silver_path:
            os.makedirs(self.config.silver_path, exist_ok=True)

    @singledispatchmethod
    async def check_if_entry_exists(self, entry) -> bool:
        raise NotImplementedError(f"No check for type {type(entry)}")

    @check_if_entry_exists.register
    async def _(self, entry: BronzeStorageEntry) -> bool:
        self._ensure_bronze_metadata_table()

        dt = DeltaTable(self.config.bronze_metadata_path)

        matching_data = dt.to_pyarrow_table(
            filters=[("url", "=", entry.url)]
            # I dont use hashes since through experimentation I found the page to change automatically every once in a while without the offer changing since recommendations or aggregate statistics change. Instead I plan to implement and update mechanism later on...
        )

        return matching_data.num_rows > 0

    @check_if_entry_exists.register
    async def _(self, entry: RawScrapedPage) -> bool:
        return await self.check_if_entry_exists(
            BronzeStorageEntry.from_scraped_page(entry)
        )

    @singledispatchmethod
    async def write_batch_of_entries(self, batch):
        raise NotImplementedError(f"No write batch for type {type(batch)}")

    @write_batch_of_entries.register
    async def _(self, batch: list):
        if not batch:
            return
        first_item = batch[0]

        # RawScrapedPage is passed since converting to BronzeStorageEntry loses information.
        # I find the best implementation to be one that uses BronzeStorageEntry instead.
        # TODO: Make _write_to_bronze_batch use BronzeStorageEntry instead, separating processing from storage.

        if isinstance(first_item, RawScrapedPage):
            await self._write_to_bronze_batch(batch)
        if isinstance(first_item, SilverStorageEntry):
            await self._write_to_silver_batch(batch)

    async def _write_to_silver_batch(self, batch: List[SilverStorageEntry]):
        pass

    async def _write_to_bronze_batch(self, batch: List[RawScrapedPage]):
        sem = asyncio.Semaphore(25)

        async def write_file(raw_page: RawScrapedPage, metadata: BronzeStorageEntry):
            async with sem:
                full_path = Path(self.config.bronze_path / metadata.file_path)
                full_path.parent.mkdir(parents=True, exist_ok=True)

                async with aiofiles.open(full_path, mode="wb") as f:
                    await f.write(raw_page.raw_content)

        async def write_metadata_batch(metadata_batch: List[BronzeStorageEntry]):
            df = pd.DataFrame([metadata.model_dump() for metadata in metadata_batch])

            write_deltalake(self.config.bronze_metadata_path, df, mode="append")

        self._ensure_bronze_metadata_table()

        required_info = [
            (raw, BronzeStorageEntry.from_scraped_page(raw)) for raw in batch
        ]
        _, metadata_list = zip(*required_info)

        write_files_tasks = [
            write_file(raw, metadata) for raw, metadata in required_info
        ]
        write_files_results = await asyncio.gather(
            *write_files_tasks, return_exceptions=True
        )  # asyncio.gather preserves coroutine order in the result list.

        written_files_metadata: List[BronzeStorageEntry] = list()

        for (raw, metadata), result in zip(required_info, write_files_results):
            if isinstance(result, Exception):
                raise result  # TODO: Log the error. The idea is to process as much as possible in the event of failure.
            else:
                written_files_metadata.append(metadata)

        if written_files_metadata:
            await write_metadata_batch(written_files_metadata)

    def read_bronze_entries(self) -> pd.DataFrame:
        self._ensure_bronze_metadata_table()
        df: pd.DataFrame = DeltaTable(self.config.bronze_metadata_path).to_pandas()

        return df

    def _ensure_bronze_metadata_table(self):
        self.config.bronze_metadata_path.mkdir(parents=True, exist_ok=True)

        if DeltaTable.is_deltatable(str(self.config.bronze_metadata_path)):
            return

        else:
            # It would be better if this schema could be programatically generated from BronzeStorageEntry instead.
            bronze_metadata_schema = pa.schema(
                [
                    ("url", pa.string()),
                    ("source_system", pa.string()),
                    ("ingestion_ts", pa.timestamp(unit="us")),
                    ("file_hash", pa.string()),
                    ("file_path", pa.string()),
                    ("file_size_bytes", pa.int32()),
                    ("year", pa.string()),
                    ("month", pa.string()),
                    ("day", pa.string()),
                ]
            )

            DeltaTable.create(
                table_uri=self.config.bronze_metadata_path,
                schema=bronze_metadata_schema,
                mode="overwrite",
            )


class Parser:
    @staticmethod
    @abstractmethod
    def parse(content: str, url: str) -> Optional[SilverStorageEntry]:
        pass


class SiteAParser(Parser):
    @staticmethod
    def parse(content: str, url: str) -> Optional[SilverStorageEntry]:
        soup = BeautifulSoup(content, "html.parser")

        loopa_data_dict: Optional[Dict[str, Any]] = SiteAParser._get_loopa_data_dict(
            soup
        )
        details_dict: Optional[Dict[str, Any]] = SiteAParser._get_details_dict(soup)
        insights_dict: Optional[Dict[str, Any]] = SiteAParser._get_insights_dict(soup)
        data_layer_dict: Optional[Dict[str, Any]] = SiteAParser._get_data_layer_dict(
            soup
        )

        offer_id = SiteAParser._get_offer_id(url)
        offer_date = SiteAParser._get_offer_date(soup, details_dict)
        offer_type = SiteAParser._get_offer_type(loopa_data_dict)
        property_type = SiteAParser._get_property_type(url)
        price = SiteAParser._get_offer_price(soup)
        total_sqm = SiteAParser._get_total_sqm(insights_dict, details_dict)
        built_sqm = SiteAParser._get_built_sqm(insights_dict, details_dict)
        chilean_region_name = SiteAParser._get_chilean_region_name(
            data_layer_dict, loopa_data_dict
        )
        chilean_location_name = SiteAParser._get_chilean_location_name(
            data_layer_dict, loopa_data_dict
        )
        number_of_rooms = SiteAParser._get_number_of_rooms(
            insights_dict, details_dict, loopa_data_dict
        )
        number_of_bathrooms = SiteAParser._get_number_of_bathrooms(
            insights_dict, details_dict
        )
        number_of_parking_spots = SiteAParser._get_number_of_parking_spots(
            insights_dict, details_dict
        )

        lat_lon = SiteAParser._get_lat_lon(soup)

        lat, lon = lat_lon if lat_lon else (None, None)

        if offer_id is None or price is None:
            return None

        return SilverStorageEntry(
            offer_id=offer_id,
            offer_date=offer_date,
            offer_type=offer_type,
            property_type=property_type,
            price=price,
            total_sqm=total_sqm,
            built_sqm=built_sqm,
            chilean_region_name=chilean_region_name,
            chilean_location_name=chilean_location_name,
            number_of_rooms=number_of_rooms,
            number_of_bathrooms=number_of_bathrooms,
            number_of_parking_spots=number_of_parking_spots,
            latitude=lat,
            longitude=lon,
        )

    @staticmethod
    def _get_offer_id(url: str) -> Optional[int]:
        return None
        match = re.search(r"/(\d+)(?:\?|$)", url)
        return int(match.group(1)) if match else None

    @staticmethod
    def _get_offer_date(soup, details_dict) -> Optional[str]:
        pass

    @staticmethod
    def _get_offer_type(loopa_data: Dict[str, Any]) -> Optional[str]:
        pass

    @staticmethod
    def _get_property_type(url: str) -> Optional[str]:
        pass

    @staticmethod
    def _get_offer_price(soup) -> Optional[str]:
        pass

    @staticmethod
    def _get_total_sqm(insights_dict, details_dict):
        pass

    @staticmethod
    def _get_built_sqm(insights_dict, details_dict):
        pass

    @staticmethod
    def _get_chilean_region_name(data_layer, loopa_data):
        pass

    @staticmethod
    def _get_chilean_location_name(data_layer, loopa_data):
        pass

    @staticmethod
    def _get_number_of_rooms(insights_dict, details_dict, loopa_data):
        pass

    @staticmethod
    def _get_number_of_bathrooms(insights_dict, details_dict):
        pass

    @staticmethod
    def _get_number_of_parking_spots(insights_dict, details_dict):
        pass

    @staticmethod
    def _get_lat_lon(soup) -> Tuple[float, float]:
        pass

    # Processing the HTML
    @staticmethod
    def _get_loopa_data_dict(soup):
        pass

    @staticmethod
    def _get_data_layer_dict(soup):
        pass

    @staticmethod
    def _get_insights_dict(soup):
        pass

    @staticmethod
    def _get_details_dict(soup):
        pass


class Processor:
    @staticmethod
    @singledispatchmethod
    def convert_entry(self, entry):
        raise NotImplementedError(
            f"'convert_entry' not implemented for entry of type {type(entry)}"
        )

    @staticmethod
    @convert_entry.register
    def convert_entry(
        entry: BronzeStorageEntry,
        file_content: str,
        parsers_dict: Dict[str, Parser],
    ) -> Optional[SilverStorageEntry]:
        """
        Takes a BronzeStorageEntry, the file_content as a string, and a parsers_dict = {'source_system': Parser}, and applies the parser that matches the source_system of the entry to generate and return a SilverStorageEntry.
        """

        right_parser: Optional[Parser] = parsers_dict.get(entry.source_system)

        if right_parser:
            return right_parser.parse(entry.url, file_content)
