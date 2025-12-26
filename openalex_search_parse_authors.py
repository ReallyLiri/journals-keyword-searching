import asyncio
import csv
import json
from pathlib import Path
from collections import defaultdict

import aiohttp
from tqdm.asyncio import tqdm

RESULTS_DIR = 'search_results'
JOURNALS_FILE = 'journals.csv'
OUTPUT_AUTHORS_FILE = 'authors.csv'

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


def _aggregate_authors_data(json_data, journal_mapping):
    authors_aggregated = defaultdict(lambda: {
        'name': '',
        'institutions': set(),
        'countries': set(),
        'affiliations': set(),
        'journal_names': set(),
        'years': set(),
        'count': 0
    })

    for item in json_data:
        publication_date = item.get('publication_date', '')
        year = publication_date[:4] if publication_date else 'Unknown'

        # Get journal name for this work
        primary_location = item.get('primary_location', {})
        source = primary_location.get('source', {}) if primary_location else {}
        source_id = source.get('id', '').replace('https://openalex.org/', '') if source else ''
        journal_name = journal_mapping.get(source_id, '')

        for authorship in item.get('authorships', []):
            author = authorship.get('author', {})
            if not author:
                continue

            author_id = author.get('id', '') if author else ''
            if author_id:
                author_id = author_id.replace('https://openalex.org/', '')
            author_name = authorship.get('raw_author_name', '')

            # Strip leading/trailing apostrophes and quotes
            author_name = author_name.strip("'\"ʻʼ'ʽ`´")

            if not author_id:
                continue

            authors_aggregated[author_id]['name'] = author_name
            authors_aggregated[author_id]['years'].add(year)

            institutions = authorship.get('institutions', [])
            has_institutions = False
            for inst in institutions:
                if inst and inst.get('display_name'):
                    authors_aggregated[author_id]['institutions'].add(inst.get('display_name'))
                    has_institutions = True

            countries = authorship.get('countries', [])
            for country in countries:
                if country:
                    authors_aggregated[author_id]['countries'].add(country)

            if not has_institutions:
                raw_affiliation_strings = authorship.get('raw_affiliation_strings', [])
                for affiliation in raw_affiliation_strings:
                    if affiliation and affiliation != "View further author information":
                        authors_aggregated[author_id]['affiliations'].add(affiliation)

            if journal_name:
                authors_aggregated[author_id]['journal_names'].add(journal_name)

            authors_aggregated[author_id]['count'] += 1

    return authors_aggregated


async def fetch_author_info(session, author_id, rate_limiter):
    async with rate_limiter:
        url = f"https://api.openalex.org/authors/{author_id}"
        params = {'mailto': 'reallyliri@gmail.com'}

        async with session.get(url, params=params) as response:
            if response.status != 200:
                print(f"Warning: HTTP {response.status} for author {author_id}")
                return None
            return await response.json()


async def enrich_authors_data(authors_rows):
    semaphore = asyncio.Semaphore(CONCURRENCY)
    rate_limiter = asyncio.Semaphore(RATE_LIMIT)

    async def enrich_single_author(row):
        async with semaphore:
            author_id = row['id']
            author_info = await fetch_author_info(session, author_id, rate_limiter)

            if author_info:
                row['total_works_count'] = author_info.get('works_count', 0)
                row['total_cited_by_count'] = author_info.get('cited_by_count', 0)
            else:
                row['total_works_count'] = 0
                row['total_cited_by_count'] = 0

            return row

    async with aiohttp.ClientSession() as session:
        enriched_rows = await tqdm.gather(
            *[enrich_single_author(row) for row in authors_rows],
            desc="Enriching authors"
        )

    return enriched_rows


def normalize_and_merge_authors(enriched_rows):
    # Step 1: Normalize author names (convert all caps to title case)
    for row in enriched_rows:
        name = row['name']
        if name and name.isupper():
            row['name'] = name.title()

    # Step 2: Group by normalized name and merge
    grouped_authors = defaultdict(list)
    for row in enriched_rows:
        grouped_authors[row['name']].append(row)

    merged_rows = []
    for name, rows in grouped_authors.items():
        if len(rows) == 1:
            merged_rows.append(rows[0])
        else:
            # Merge multiple rows with same name
            merged_row = {
                'name': name,
                'id': ';'.join(row['id'] for row in rows),
                'years': ';'.join(sorted(set(year for row in rows for year in row['years'].split(';') if year))),
                'institutions': ';'.join(sorted(set(inst for row in rows for inst in row['institutions'].split(';') if inst))),
                'countries': ';'.join(sorted(set(country for row in rows for country in row['countries'].split(';') if country))),
                'affiliations_comment': ';'.join(sorted(set(aff for row in rows for aff in row['affiliations_comment'].split(';') if aff))),
                'journal_names': ';'.join(sorted(set(journal for row in rows for journal in row['journal_names'].split(';') if journal))),
                'works_count': sum(row['works_count'] for row in rows),
                'total_works_count': sum(row['total_works_count'] for row in rows),
                'total_cited_by_count': sum(row['total_cited_by_count'] for row in rows)
            }
            merged_rows.append(merged_row)

    return merged_rows




def main():
    results_dir = Path(RESULTS_DIR)
    all_json_data = []

    journal_mapping = _load_journal_mapping()

    for json_file in results_dir.glob('*.json'):
        print(f"Processing {json_file.name}...")
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            all_json_data.extend(json_data)

    authors_aggregated = _aggregate_authors_data(all_json_data, journal_mapping)

    authors_rows = []
    for author_id, data in authors_aggregated.items():
        row = {
            'id': author_id,
            'name': data['name'],
            'years': ';'.join(sorted(data['years'])),
            'institutions': ';'.join(sorted(data['institutions'])),
            'countries': ';'.join(sorted(data['countries'])),
            'affiliations_comment': ';'.join(sorted(data['affiliations'])),
            'journal_names': ';'.join(sorted(data['journal_names'])),
            'works_count': data['count']
        }
        authors_rows.append(row)

    authors_rows.sort(key=lambda x: x['name'])

    if authors_rows:
        print(f"Enriching {len(authors_rows)} authors with API data...")
        enriched_rows = asyncio.run(enrich_authors_data(authors_rows))

        print(f"Normalizing and merging authors by name...")
        final_rows = normalize_and_merge_authors(enriched_rows)

        with open(OUTPUT_AUTHORS_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['id', 'name', 'years', 'institutions', 'countries', 'affiliations_comment', 'journal_names', 'works_count', 'total_works_count', 'total_cited_by_count']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(final_rows)

        print(f"Successfully created {OUTPUT_AUTHORS_FILE} with {len(final_rows)} records")
    else:
        print("No authors data found to process")


if __name__ == "__main__":
    main()
