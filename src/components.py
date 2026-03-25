from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs

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
import json
import html


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
    REGIONS_WITH_ACCENTS: Dict[str, str] = {
        "Region Metropolitana": "Región Metropolitana",
        "Arica Paranicota": "Arica & Paranicota",
        "Tarapaca": "Tarapacá",
        "Valparaiso": "Valparaíso",
        "Ohiggins": "O'Higgins",
        "Nuble": "Ñuble",
        "Biobio": "Biobío",
        "Araucania": "Araucanía",
        "Los Rios": "Los Ríos",
        "Magallanes Antartica": "Magallanes & Antártica",
    }

    PROPERTY_TYPE_MAP: Dict[str, str] = {"Apartments": "Apartamento", "Houses": "Casa"}

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
        offer_date = SiteAParser._get_offer_date(details_dict)
        offer_type = SiteAParser._get_offer_type(loopa_data_dict)
        property_type = SiteAParser._get_property_type(loopa_data_dict)
        price = SiteAParser._get_offer_price(soup, insights_dict)
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
        match = re.search(r"/(\d+)(?:\?|$)", url)
        return int(match.group(1)) if match else None

    @staticmethod
    def _get_offer_date(details_dict: Dict[str, str]) -> Optional[str]:
        details_date = details_dict.get("Publicado")

        if details_date:
            return details_date

    @staticmethod
    def _get_offer_type(loopa_data: Dict[str, Any]) -> Optional[str]:
        offer_type = loopa_data.get("SaleType")

        if offer_type == "SALE":
            return "Venta"

    @staticmethod
    def _get_property_type(loopa_data: Dict[str, str]) -> Optional[str]:
        system_housing_type = loopa_data.get("HousingType")

        if system_housing_type:
            property_type = SiteAParser.PROPERTY_TYPE_MAP.get(system_housing_type)
            return property_type

        return None

    @staticmethod
    def _get_offer_price(soup, insights_dict: Dict[str, str]) -> Optional[str]:
        raw_price = insights_dict.get("Precio")
        if not raw_price:
            return None

        match = re.search(r"(?:UF|\$)\d{1,3}(?:\.\d{3})*(?:,\d{2})?", raw_price)
        if match:
            return match.group(0)

        return None

    @staticmethod
    def _get_total_sqm(
        insights_dict: Dict[str, str], details_dict: Dict[str, str]
    ) -> Optional[float]:
        total_sqm = details_dict.get("M² totales")

        if total_sqm:
            try:
                return float(total_sqm)
            except ValueError:
                pass

        return None

    @staticmethod
    def _get_built_sqm(
        insights_dict: Dict[str, str], details_dict: Dict[str, str]
    ) -> Optional[float]:
        built_sqm = insights_dict.get("Área construida (m²)")

        if built_sqm:
            return float(built_sqm)
        else:
            return None

    @staticmethod
    def _get_chilean_region_name(
        data_layer: Dict[str, str], loopa_data: Dict[str, str]
    ):
        region = data_layer.get("province")

        if not region:
            return None

        match = re.search(r"^chile-es-(.+)$", region)
        if match:
            formatted = match.group(1).replace("-", " ").title()

            with_accents = SiteAParser.REGIONS_WITH_ACCENTS.get(formatted)

            if with_accents:
                return with_accents

            else:
                return formatted

        return None

    @staticmethod
    def _get_chilean_location_name(
        data_layer: Dict[str, str], loopa_data: Dict[str, str]
    ) -> Optional[str]:
        # If province and location are not duplicated on data_layer, then loopa_data's region must be the 'location'. (Quirks of the source)
        province = data_layer.get("province")
        location = data_layer.get("location")

        if province == location:
            return None

        else:
            return loopa_data.get("Region")

    @staticmethod
    def _get_number_of_rooms(
        insights_dict: Dict[str, str],
        details_dict: Dict[str, str],
        loopa_data: Dict[str, str],
    ):
        insights_rooms = insights_dict.get("Dormitorios")

        if insights_rooms:
            try:
                return int(insights_rooms)
            except ValueError:
                pass

        else:
            return None

    @staticmethod
    def _get_number_of_bathrooms(
        insights_dict: Dict[str, str], details_dict: Dict[str, str]
    ) -> Optional[float]:
        insights_bathrooms = insights_dict.get("Baños")

        if insights_bathrooms:
            try:
                return float(insights_bathrooms)
            except ValueError:
                pass

        else:
            return None

    @staticmethod
    def _get_number_of_parking_spots(
        insights_dict: Dict[str, str], details_dict: Dict[str, str]
    ) -> Optional[int]:
        insights_parking_spots = insights_dict.get("Estacionamientos")

        if insights_parking_spots:
            try:
                return int(insights_parking_spots)
            except ValueError:
                return None

        else:
            return None

    @staticmethod
    def _get_lat_lon(soup) -> Tuple[Optional[float], Optional[float]]:
        iframe = soup.select_one("section.d3-property__map iframe")

        if iframe and iframe.has_attr("src"):
            src_url = html.unescape(iframe["src"])
            query = parse_qs(urlparse(src_url).query)
            if "q" in query:
                coords = query["q"][0].split(",")
                if len(coords) == 2:
                    try:
                        lat, lng = float(coords[0]), float(coords[1])
                        return lat, lng
                    except ValueError:
                        pass

        return None, None

    # Processing the HTML
    @staticmethod
    def _get_loopa_data_dict(soup) -> Dict[str, str]:
        script = soup.find(string=re.compile(r"loopaData"))
        data = {}

        if script:
            match = re.search(
                r"var\s+loopaData\s*=\s*({.*?});", script.string, re.DOTALL
            )
            if match:
                json_str = match.group(1)
                data = json.loads(json_str)

        return data

    @staticmethod
    def _get_data_layer_dict(soup) -> Dict[str, str]:

        data = {}

        script = soup.find(string=re.compile(r"dataLayer\.push"))

        if script:
            match = re.search(
                r"dataLayer\.push\(\s*({.*?})\s*\);", script.string, re.DOTALL
            )
            if match:
                json_str = match.group(1)
                data = json.loads(json_str)

        return data

    @staticmethod
    def _get_insights_dict(soup) -> Dict[str, str]:
        insights = soup.select(".d3-property-insight__attribute-details")

        result = {}

        for insight in insights:
            key = insight.select_one(".d3-property-insight__attribute-title")
            value = insight.select_one(".d3-property-insight__attribute-value")

            if not key or not value:
                continue

            key = key.getText()
            value = value.getText()

            result[key] = value

        return result

    @staticmethod
    def _get_details_dict(soup) -> Dict[str, str]:
        data = {}

        for item in soup.select(".d3-property-details__detail-label"):
            label = item.contents[0].get_text(strip=True)

            value_tag = item.select_one(".d3-property-details__detail")
            value = value_tag.get_text(strip=True) if value_tag else ""

            data[label] = value

        return data


class Processor:
    @singledispatchmethod
    def convert_entry(self, entry):
        raise NotImplementedError(
            f"'convert_entry' not implemented for entry of type {type(entry)}"
        )

    @convert_entry.register
    def convert_entry(
        self,
        entry: BronzeStorageEntry,
        file_content: str,
        parsers_dict: Dict[str, Parser],
    ) -> Optional[SilverStorageEntry]:
        """
        Takes a BronzeStorageEntry, the file_content as a string, and a parsers_dict = {'source_system': Parser}, and applies the parser that matches the source_system of the entry to generate and return a SilverStorageEntry.
        """

        right_parser: Optional[Parser] = parsers_dict.get(entry.source_system)

        if right_parser:
            return right_parser.parse(file_content, entry.url)
