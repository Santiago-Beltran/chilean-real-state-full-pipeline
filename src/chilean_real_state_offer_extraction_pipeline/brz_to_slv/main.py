"""
Reads from $BRONZE_DELTALAKE_LOCATION and writes to $SILVER_DELTALAKE_LOCATION.

Ingestion Pattern: Full Loader
"""

import logging
import os
import math
from typing import List, Dict, Any, Iterable, Optional

import pandas as pd
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from dotenv import load_dotenv

from chilean_real_state_offer_extraction_pipeline.models import BronzeStorageEntry, SilverStorageEntry
from chilean_real_state_offer_extraction_pipeline.components import Processor, SiteAParser

load_dotenv()

logger = logging.getLogger(__name__)

BRONZE_DELTALAKE_LOCATION = os.getenv("BRONZE_DELTALAKE_LOCATION", "data/bronze")
SILVER_DELTALAKE_LOCATION = os.getenv("SILVER_DELTALAKE_LOCATION", "data/silver")


# TODO: Add URL and file_hash to silver_row for debugging.
def run_process() -> Optional[str]:
    builder = (
        SparkSession.builder.appName("MyApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df = spark.read.format("delta").load(BRONZE_DELTALAKE_LOCATION + "/bronze_metadata")

    number_of_rows_per_partition = 50
    total_rows = df.count()

    num_partitions = (
        math.ceil(total_rows / number_of_rows_per_partition) if total_rows > 0 else 1
    )

    df_repart = df.repartition(num_partitions)
    processor = Processor()

    def process_bronze_batch(pdf: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
        for df in pdf:
            new_rows: List[Dict[str, Any]] = list()

            for _, row in df.iterrows():
                try:
                    bronze_row: BronzeStorageEntry = BronzeStorageEntry(**row.to_dict())
                except Exception as e:
                    logger.error(
                        f"Could not process row: {row.to_dict()}. Exception: {e}"
                    )
                    continue

                with open(
                    f"{BRONZE_DELTALAKE_LOCATION}/{bronze_row.file_path}", "r"
                ) as f:
                    content: str = f.read()

                silver_entry: Optional[SilverStorageEntry] = processor.convert_entry(
                    entry=bronze_row,
                    file_content=content,
                    parsers_dict={"SiteA": SiteAParser},
                )

                if silver_entry:
                    new_rows.append(silver_entry.model_dump())

            yield pd.DataFrame(new_rows)

    silver_schema_ddl = """
        offer_id LONG,
        offer_date STRING,
        offer_type STRING,
        property_type STRING,
        price STRING,
        total_sqm DOUBLE,
        built_sqm DOUBLE,
        chilean_region_name STRING,
        chilean_location_name STRING,
        number_of_rooms INT,
        number_of_bathrooms FLOAT,
        number_of_parking_spots INT,
        latitude DOUBLE,
        longitude DOUBLE
    """

    if total_rows > 0:
        processed_df = df_repart.mapInPandas(
            process_bronze_batch, schema=silver_schema_ddl
        )

        processed_df.write.format("delta").mode("overwrite").save(
            SILVER_DELTALAKE_LOCATION
        )

    else:
        logger.info("Bronze metadata is empty. No files processed.")

    return "SUCCESS"

run_process()
