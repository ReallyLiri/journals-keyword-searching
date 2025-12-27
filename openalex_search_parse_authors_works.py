import asyncio
import csv
import json
from pathlib import Path
from collections import defaultdict

import aiohttp
from tqdm.asyncio import tqdm

RESULTS_DIR = 'search_results'
JOURNALS_FILE = 'journals.csv'
AUTHORS_FILE = 'authors.csv'
OUTPUT_AUTHORS_WORKS_FILE = 'authors_works.csv'

CONCURRENCY = 5
RATE_LIMIT = 10


def _load_journal_mapping():
    journal_mapping = {}
    with open(JOURNALS_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_id = row['OpenAlexSourceId']
            journal_name = row['Journal Name']
            journal_mapping[source_id] = journal_name
    return journal_mapping


def _load_authors_data():
    authors_data = {}
    alias_to_primary = {}  # Maps alias IDs to primary (first) ID
    primary_to_name = {}   # Maps primary ID to author name

    with open(AUTHORS_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ids_list = [id.strip() for id in row['id'].split(';') if id.strip()]
            names_list = row['name'].split(';')

            if not ids_list:
                continue

            # First ID is considered the primary ID
            primary_id = ids_list[0]

            # Get the author name (use first name or the only name)
            if len(names_list) == 1:
                author_name = names_list[0].strip()
            else:
                author_name = names_list[0].strip() if names_list else ''

            primary_to_name[primary_id] = author_name

            # Map all IDs (including primary) to the primary ID
            for author_id in ids_list:
                alias_to_primary[author_id] = primary_id
                authors_data[author_id] = author_name

    return authors_data, alias_to_primary, primary_to_name


def _load_journal_ids():
    journal_ids = set()
    with open(JOURNALS_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_id = row['OpenAlexSourceId']
            if source_id.strip():
                journal_ids.add(source_id.strip())
    return journal_ids


async def fetch_author_works_by_journal(session, author_id, rate_limiter):
    async with rate_limiter:
        url = f"https://api.openalex.org/works"
        params = {
            'filter': f'authorships.author.id:{author_id}',
            'group_by': 'primary_location.source.id',
            'mailto': 'reallyliri@gmail.com'
        }

        async with session.get(url, params=params) as response:
            if response.status != 200:
                print(f"Warning: HTTP {response.status} for author {author_id}")
                return {'author_id': author_id, 'journal_counts': {}}
            data = await response.json()

            journal_counts = {}
            group_by_results = data.get('group_by', [])
            for item in group_by_results:
                journal_id = item.get('key', '')
                if journal_id:
                    journal_id = journal_id.replace('https://openalex.org/', '')
                    journal_counts[journal_id] = item.get('count', 0)
            return {'author_id': author_id, 'journal_counts': journal_counts}


async def generate_authors_works_data(all_json_data, authors_data, alias_to_primary, primary_to_name, journal_ids, journal_mapping):
    # Count specific works from search results
    specific_works = defaultdict(int)

    for item in all_json_data:
        primary_location = item.get('primary_location', {})
        source = primary_location.get('source', {}) if primary_location else {}
        source_id = source.get('id', '').replace('https://openalex.org/', '') if source else ''

        if source_id in journal_ids:
            for authorship in item.get('authorships', []):
                author = authorship.get('author', {})
                if author:
                    author_id = author.get('id', '')
                    if author_id:
                        author_id = author_id.replace('https://openalex.org/', '')
                        if author_id in authors_data:
                            pair_key = (author_id, source_id)
                            specific_works[pair_key] += 1

    # Fetch works grouped by journal for each author
    semaphore = asyncio.Semaphore(CONCURRENCY)
    rate_limiter = asyncio.Semaphore(RATE_LIMIT)

    async def get_author_journals(author_id):
        async with semaphore:
            return await fetch_author_works_by_journal(session, author_id, rate_limiter)

    print(f"Fetching works by journal for {len(authors_data)} authors...")

    async with aiohttp.ClientSession() as session:
        tasks = [get_author_journals(author_id) for author_id in authors_data.keys()]
        journal_counts_results = await tqdm.gather(*tasks, desc="Fetching author works by journal")

    # Aggregate journal counts by primary author ID
    primary_journal_counts = defaultdict(lambda: defaultdict(int))
    for result in journal_counts_results:
        author_id = result['author_id']
        journal_counts = result['journal_counts']

        # Get the primary ID for this author
        primary_id = alias_to_primary.get(author_id, author_id)

        # Aggregate counts for each journal
        for journal_id, count in journal_counts.items():
            primary_journal_counts[primary_id][journal_id] += count

    # Also aggregate specific works by primary author ID
    primary_specific_works = defaultdict(int)
    for (author_id, journal_id), count in specific_works.items():
        primary_id = alias_to_primary.get(author_id, author_id)
        primary_specific_works[(primary_id, journal_id)] += count

    # Create output rows using primary IDs only
    authors_works_rows = []
    for primary_id in primary_to_name.keys():
        author_name = primary_to_name[primary_id]
        journal_counts = primary_journal_counts.get(primary_id, {})

        for journal_id in journal_ids:
            total_works = journal_counts.get(journal_id, 0)
            specific_count = primary_specific_works.get((primary_id, journal_id), 0)
            journal_name = journal_mapping.get(journal_id, '')

            # Only include if total_works > 0
            if total_works > 0:
                row = {
                    'author_id': primary_id,
                    'author_name': author_name,
                    'journal_id': journal_id,
                    'journal_name': journal_name,
                    'specific_works': specific_count,
                    'total_works': total_works
                }
                authors_works_rows.append(row)

    return authors_works_rows


def main():
    results_dir = Path(RESULTS_DIR)
    all_json_data = []

    journal_mapping = _load_journal_mapping()
    authors_data, alias_to_primary, primary_to_name = _load_authors_data()
    journal_ids = _load_journal_ids()

    print(f"Loaded {len(primary_to_name)} unique authors ({len(authors_data)} total IDs) and {len(journal_ids)} journal IDs")

    for json_file in results_dir.glob('*.json'):
        print(f"Processing {json_file.name}...")
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            all_json_data.extend(json_data)

    # Generate authors_works.csv
    print("Generating author-journal works data...")
    authors_works_rows = asyncio.run(generate_authors_works_data(all_json_data, authors_data, alias_to_primary, primary_to_name, journal_ids, journal_mapping))

    if authors_works_rows:
        authors_works_rows.sort(key=lambda x: (x['author_id'], x['journal_id']))

        with open(OUTPUT_AUTHORS_WORKS_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['author_id', 'author_name', 'journal_id', 'journal_name', 'specific_works', 'total_works']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(authors_works_rows)

        print(f"Successfully created {OUTPUT_AUTHORS_WORKS_FILE} with {len(authors_works_rows)} records")
    else:
        print("No author-journal data found to process")


if __name__ == "__main__":
    main()
