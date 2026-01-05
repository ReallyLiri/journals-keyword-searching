import asyncio
import csv
import json
from pathlib import Path
from collections import defaultdict

import aiohttp
from tqdm.asyncio import tqdm

RESULTS_DIR = 'search_results'
OUTPUT_AUTHORS_FILE = 'authors_works.csv'

CONCURRENCY = 5
RATE_LIMIT = 10


def _load_search_results_work_ids():
    work_ids = set()
    results_dir = Path(RESULTS_DIR)
    for json_file in results_dir.glob('*.json'):
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            for item in json_data:
                work_id = item.get('id', '').replace('https://openalex.org/', '')
                if work_id:
                    work_ids.add(work_id)
    return work_ids


def _extract_author_works(json_data):
    author_works = defaultdict(list)

    for item in json_data:
        for authorship in item.get('authorships', []):
            author = authorship.get('author', {})
            if not author:
                continue

            author_id = author.get('id', '')
            if author_id:
                author_id = author_id.replace('https://openalex.org/', '')
                if author_id:
                    author_works[author_id].append(item)

    return author_works


def _extract_work_data(item, search_work_ids):
    row = {}

    openalex_prefix = 'https://openalex.org/'
    row['id'] = item.get('id', '').replace(openalex_prefix, '')
    row['doi'] = item.get('doi', '')
    row['title'] = item.get('title', '')
    row['publication_date'] = item.get('publication_date', '')

    primary_location = item.get('primary_location', {})
    source = primary_location.get('source', {}) if primary_location else {}
    row['source_id'] = source.get('id', '').replace(openalex_prefix, '') if source else ''
    row['journal_name'] = source.get('display_name', '') if source else ''

    authors = item.get('authorships', [])
    author_names = [author.get('raw_author_name', '').strip("'\"ʻʼ'ʽ`´")
                    for author in authors if author.get('raw_author_name')]
    row['authors'] = ';'.join(author_names)

    author_ids = []
    for authorship in authors:
        author = authorship.get('author', {})
        if author:
            author_id = author.get('id', '')
            if author_id:
                author_id = author_id.replace(openalex_prefix, '')
                if author_id:
                    author_ids.append(author_id)
    row['author_ids'] = ';'.join(author_ids)

    row['cited_by_count'] = item.get('cited_by_count', 0)

    keywords = item.get('keywords', [])
    keyword_names = [kw.get('display_name', '') for kw in keywords if kw.get('display_name')]
    row['keywords'] = ';'.join(keyword_names)

    row['references_israel'] = 'Yes' if row['id'] in search_work_ids else 'No'

    return row


async def fetch_author_works(session, author_id, rate_limiter):
    async with rate_limiter:
        url = f"https://api.openalex.org/works"
        params = {
            'filter': f'author.id:{author_id}',
            'per-page': 200,
            'mailto': 'reallyliri@gmail.com'
        }

        all_works = []
        cursor = '*'

        while cursor:
            params['cursor'] = cursor
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    print(f"Warning: HTTP {response.status} for author {author_id}")
                    break

                data = await response.json()
                results = data.get('results', [])
                all_works.extend(results)

                meta = data.get('meta', {})
                next_cursor = meta.get('next_cursor')
                cursor = next_cursor if next_cursor else None

                if len(results) < 200:
                    break

        return all_works


async def enrich_authors_with_all_works(author_ids):
    semaphore = asyncio.Semaphore(CONCURRENCY)
    rate_limiter = asyncio.Semaphore(RATE_LIMIT)

    async def fetch_single_author_works(author_id):
        async with semaphore:
            return await fetch_author_works(session, author_id, rate_limiter)

    async with aiohttp.ClientSession() as session:
        all_author_works = await tqdm.gather(
            *[fetch_single_author_works(author_id) for author_id in author_ids],
            desc="Fetching all works for authors"
        )

    author_works_dict = {}
    for author_id, works in zip(author_ids, all_author_works):
        if works:
            author_works_dict[author_id] = works

    return author_works_dict


def main():
    results_dir = Path(RESULTS_DIR)
    all_json_data = []

    print("Loading search results work IDs...")
    search_work_ids = _load_search_results_work_ids()
    print(f"Found {len(search_work_ids)} unique work IDs in search results")

    for json_file in results_dir.glob('*.json'):
        print(f"Processing {json_file.name}...")
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            all_json_data.extend(json_data)

    author_works_from_search = _extract_author_works(all_json_data)
    unique_author_ids = list(author_works_from_search.keys())

    print(f"Found {len(unique_author_ids)} unique authors in search results")

    if unique_author_ids:
        print(f"Fetching all works for {len(unique_author_ids)} authors from OpenAlex API...")
        all_author_works = asyncio.run(enrich_authors_with_all_works(unique_author_ids))

        seen_work_ids = set()
        output_rows = []

        for author_id in unique_author_ids:
            if author_id in all_author_works:
                works = all_author_works[author_id]
                for work in works:
                    work_data = _extract_work_data(work, search_work_ids)

                    if work_data['id'] and work_data['id'] not in seen_work_ids:
                        seen_work_ids.add(work_data['id'])
                        output_rows.append(work_data)

        if output_rows:
            with open(OUTPUT_AUTHORS_FILE, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'id', 'doi', 'title', 'publication_date', 'source_id',
                    'journal_name', 'authors', 'author_ids', 'cited_by_count', 'keywords', 'references_israel'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                writer.writerows(output_rows)

            print(f"Successfully created {OUTPUT_AUTHORS_FILE} with {len(output_rows)} unique works")
        else:
            print("No works data found to process")
    else:
        print("No authors data found to process")


if __name__ == "__main__":
    main()
