import asyncio
import csv
import sys

import aiohttp
from tqdm.asyncio import tqdm

CSV_FILE = "journals.csv"
OUTPUT_FILE = "journal_stats.csv"
FROM_YEAR = 1940
CONCURRENCY = 1
RATE_LIMIT = 10
SEARCH_PHRASES = [
    None,
    "israel",
    "australia",
    "france",
    "belgium",
    "syria",
    "lebanon",
    "egypt",
    "iraq",
    "(\"south africa\")",
    "germany",
    "japan",
    "italy",
    "(\"united states\" OR usa)",
    "spain",
    "jordan",
    "algeria",
    "switzerland",
]


async def fetch_stats(session, source_id, phrase, rate_limiter):
    async with rate_limiter:
        url = f"https://api.openalex.org/works"

        filter_param = f'primary_location.source.id:{source_id},from_publication_date:{FROM_YEAR}-01-01'
        if phrase:
            filter_param = f'title_and_abstract.search:{phrase},{filter_param}'

        params = {
            'filter': filter_param,
            'group_by': 'publication_year',
            'mailto': 'reallyliri@gmail.com'
        }

        async with session.get(url, params=params) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status} for source {source_id} with phrase '{phrase}'")
            data = await response.json()
            return data


def _col_name(phrase) -> str:
    if not phrase:
        return "count"
    if phrase.startswith("("):
        phrase = phrase[1:-1].split("\" OR")[0].replace(" ", "_").replace('"', "")
    return f"count_{phrase}"


async def process_source(session, source_id, journal_name, semaphore, rate_limiter):
    async with semaphore:
        try:
            results_by_year = {}

            for phrase in SEARCH_PHRASES:
                data = await fetch_stats(session, source_id, phrase, rate_limiter)
                group_by_results = data.get('group_by', [])

                column_name = _col_name(phrase)

                for item in group_by_results:
                    year = item.get('key')
                    count = item.get('count', 0)
                    if year:
                        if year not in results_by_year:
                            results_by_year[year] = {
                                'source_id': source_id,
                                'journal_name': journal_name,
                                'year': year
                            }
                        results_by_year[year][column_name] = count

            for year_data in results_by_year.values():
                for phrase in SEARCH_PHRASES:
                    column_name = _col_name(phrase)
                    if column_name not in year_data:
                        year_data[column_name] = 0

            return list(results_by_year.values())

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
            fieldnames = ['source_id', 'journal_name', 'year']
            for phrase in SEARCH_PHRASES:
                column_name = _col_name(phrase)
                fieldnames.append(column_name)

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)

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
