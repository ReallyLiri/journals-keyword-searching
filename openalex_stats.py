import sys

import aiohttp
import asyncio
import csv
from tqdm.asyncio import tqdm

CSV_FILE = "journals.csv"
OUTPUT_FILE = "journal_stats.csv"
CONCURRENCY = 1
RATE_LIMIT = 10


async def fetch_stats(session, source_id, rate_limiter):
    async with rate_limiter:
        url = f"https://api.openalex.org/works"
        params = {
            'filter': f'primary_location.source.id:{source_id}',
            'group_by': 'publication_year',
            'mailto': 'reallyliri@gmail.com'
        }

        async with session.get(url, params=params) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status} for source {source_id}")
            data = await response.json()
            return data


async def process_source(session, source_id, journal_name, semaphore, rate_limiter):
    async with semaphore:
        try:
            data = await fetch_stats(session, source_id, rate_limiter)

            results = []
            group_by_results = data.get('group_by', [])

            for item in group_by_results:
                year = item.get('key')
                count = item.get('count', 0)
                if year:
                    results.append({
                        'source_id': source_id,
                        'journal_name': journal_name,
                        'year': year,
                        'count': count
                    })

            return results

        except Exception as e:
            raise Exception(f"Error processing source {source_id}: {str(e)}")


async def main():
    try:
        journals = []

        with open(CSV_FILE, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)

            if 'OpenAlexSourceId' not in reader.fieldnames or 'Journal Name' not in reader.fieldnames:
                raise Exception("CSV file must contain 'OpenAlexSourceId' and 'Journal Name' columns")

            for row in reader:
                source_id = row['OpenAlexSourceId']
                journal_name = row['Journal Name']
                if source_id and source_id.strip():
                    source_id = source_id.strip()
                    journals.append((source_id, journal_name))

        if not journals:
            raise Exception("No valid journal entries found in CSV")

        semaphore = asyncio.Semaphore(CONCURRENCY)
        rate_limiter = asyncio.Semaphore(RATE_LIMIT)

        async with aiohttp.ClientSession() as session:
            tasks = [process_source(session, source_id, journal_name, semaphore, rate_limiter)
                     for source_id, journal_name in journals]

            results = await tqdm.gather(*tasks, desc="Processing journals")

        all_stats = []
        for result in results:
            all_stats.extend(result)

        with open(OUTPUT_FILE, 'w', newline='') as csvfile:
            fieldnames = ['source_id', 'journal_name', 'year', 'count']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for stat in all_stats:
                writer.writerow(stat)

        print(f"Statistics written to {OUTPUT_FILE}")
        print(f"Total entries: {len(all_stats)}")

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
