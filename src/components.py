from deltalake import write_deltalake, DeltaTable
import asyncio
import aiofiles
from typing import List
from pathlib import Path
from functools import singledispatchmethod

import pandas as pd
import pyarrow as pa

from src.models import RawScrapedPage, BronzeStorageEntry, SilverStorageEntry

import os


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
