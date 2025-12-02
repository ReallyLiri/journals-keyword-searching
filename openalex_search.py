import asyncio
import csv
import json
import sys
from pathlib import Path
import urllib.parse
import aiohttp
import requests
from tqdm.asyncio import tqdm

CSV_FILE = "journals.csv"
CONCURRENCY = 1
RATE_LIMIT = 10  # per second
OUTPUT_DIR = Path("search_results")
KEYWORD = "israel"

OUTPUT_DIR.mkdir(exist_ok=True)


async def fetch_page(session, source_id, page, rate_limiter):
    async with rate_limiter:
        url = f"https://api.openalex.org/works"
        params = {
            'page': page,
            'filter': f'title_and_abstract.search:{urllib.parse.quote(KEYWORD)},primary_location.source.id:{source_id}',
            'per_page': 100,
            'mailto': 'reallyliri@gmail.com'
        }

        async with session.get(url, params=params) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status} for source {source_id}, page {page}")
            data = await response.json()
            return data


async def process_source_id(session, source_id, semaphore, rate_limiter):
    async with semaphore:
        all_results = []
        page = 1

        while True:
            try:
                data = await fetch_page(session, source_id, page, rate_limiter)

                results = data.get('results', [])
                if not results:
                    break

                all_results.extend(results)

                meta = data.get('meta', {})
                if page >= meta.get('count', 0) / 100:
                    break

                page += 1

            except Exception as e:
                raise Exception(f"Error processing source {source_id}, page {page}: {str(e)}")

        output_file = f"{OUTPUT_DIR}/{source_id}.json"
        with open(output_file, 'w') as f:
            json.dump(all_results, f, indent=2)

        return len(all_results)


async def main():
    try:
        source_ids = []

        with open(CSV_FILE, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)

            if 'OpenAlexSourceId' not in reader.fieldnames:
                raise Exception("CSV file must contain 'OpenAlexSourceId' column")

            for row in reader:
                source_id = row['OpenAlexSourceId']
                if source_id and source_id.strip():
                    source_id = source_id.strip()
                    if source_id not in source_ids:
                        source_ids.append(source_id)

        if not source_ids:
            raise Exception("No valid OpenAlexSourceId values found in CSV")

        semaphore = asyncio.Semaphore(CONCURRENCY)
        rate_limiter = asyncio.Semaphore(RATE_LIMIT)

        async with aiohttp.ClientSession() as session:
            tasks = [process_source_id(session, source_id, semaphore, rate_limiter) for source_id in source_ids]

            results = await tqdm.gather(*tasks, desc="Processing sources")

        total_results = sum(results)
        print(f"Total results fetched: {total_results}")

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
