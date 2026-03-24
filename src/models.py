from pydantic import BaseModel
from datetime import datetime
from typing import Optional

import hashlib


class RawScrapedPage(BaseModel):
    url: str
    source_system: str
    ingestion_ts: datetime
    raw_content: bytes


class BronzeStorageEntry(BaseModel):
    url: str
    source_system: str
    ingestion_ts: datetime
    file_hash: str
    file_path: str
    file_size_bytes: int
    year: str
    month: str
    day: str

    @classmethod
    def from_scraped_page(cls, page: "RawScrapedPage") -> "BronzeStorageEntry":
        file_hash = hashlib.sha256(page.raw_content).hexdigest()
        file_size = len(page.raw_content)

        year = page.ingestion_ts.strftime("%Y")
        month = page.ingestion_ts.strftime("%m")
        day = page.ingestion_ts.strftime("%d")

        partition_path = (
            f"source_system={page.source_system}/year={year}/month={month}/day={day}"
        )
        file_path = f"{partition_path}/{file_hash}.html"

        return cls(
            url=page.url,
            source_system=page.source_system,
            ingestion_ts=page.ingestion_ts,
            file_hash=file_hash,
            file_path=str(file_path),
            file_size_bytes=file_size,
            year=year,
            month=month,
            day=day,
        )


class SilverStorageEntry(BaseModel):
    offer_id: int
    offer_date: Optional[str]
    offer_type: Optional[str]

    property_type: Optional[str]

    price: str
    total_sqm: Optional[float]
    built_sqm: Optional[float]

    chilean_region_name: Optional[str]
    chilean_location_name: Optional[str]

    number_of_rooms: Optional[int]
    number_of_bathrooms: Optional[int]
    number_of_parking_spots: Optional[int]

    latitude: Optional[float]
    longitude: Optional[float]
