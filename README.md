Functional MVP pipeline for extracting listing data from Chilean real estate portals

This work builds on the process defined here: https://github.com/Santiago-Beltran/pipeline-creation-workflow

For the sake of efficiency, this document focuses on the results obtained rather than detailing the full development process.

English is used throughout this project to reach a broader audience, although the original workflow documentation is available in Spanish for those interested in exploring it.

## Results
- Incremental scraper: Extracts only what hasn't been extracted yet.
- Over 70K offers processed = Over 70k rows on final dataset.
- Uses PySpark for quick processing: Processing is really fast and scalable over a cluster if the need arises.
- Idempotency from Bronze to Silver, if the process fails just re-try.
- First load, then transform: Implements Medallion Architecture, the scraper loads information to a bronze layer where the data awaits further processing. Backfilling is easy.
- Testing infrastructure: Using Pytest and some folders it's easy to evaluate the transformations applied to the .html's, if some field is missing just add a new page{n} folder on /tests/resources with the adequate information, make the changes until it runs, and re-process.

## Additional:
If you would like to know about the creation process of such pipeline, please check https://github.com/Santiago-Beltran/pipeline-creation-workflow (In Spanish).

I would greatly appreciate your feedback, I want to be better.

Have a good day.
