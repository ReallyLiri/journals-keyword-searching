import csv
import json
from pathlib import Path

RESULTS_DIR = 'search_results'
OUTPUT_FILE = 'search_results/combined.csv'
JOURNALS_FILE = 'journals.csv'


def _parse_abstract_inverted_index(abstract_inverted_index):
    if not abstract_inverted_index:
        return ""

    word_positions = []
    for word, positions in abstract_inverted_index.items():
        for position in positions:
            word_positions.append((position, word))

    word_positions.sort(key=lambda x: x[0])
    abstract = " ".join([word for _, word in word_positions])
    if "An abstract is not available for this content" in abstract:
        return ""
    return abstract


def _load_journal_mapping():
    journal_mapping = {}
    with open(JOURNALS_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_id = row['OpenAlexSourceId']
            journal_name = row['Journal Name']
            journal_mapping[source_id] = journal_name
    return journal_mapping


def _extract_data_from_json(json_data, journal_mapping):
    results = []

    for item in json_data:
        row = {}

        openalex_prefix = 'https://openalex.org/'
        row['id'] = item.get('id', '').replace(openalex_prefix, '')
        row['doi'] = item.get('doi', '')
        row['title'] = item.get('title', '')
        row['publication_date'] = item.get('publication_date', '')

        primary_location = item.get('primary_location', {})
        source = primary_location.get('source', {}) if primary_location else {}
        row['source_id'] = source.get('id', '').replace(openalex_prefix, '') if source else ''
        row['journal_name'] = journal_mapping.get(row['source_id'], '')

        open_access = item.get('open_access', {})
        row['oa_status'] = open_access.get('oa_status', '') if open_access else ''
        row['oa_url'] = open_access.get('oa_url', '') if open_access else ''

        authors = item.get('authorships', [])
        author_names = [author.get('raw_author_name') for author in authors if author.get('raw_author_name')]
        row['authors'] = ';'.join(author_names)

        row['cited_by_count'] = item.get('cited_by_count', 0)

        keywords = item.get('keywords', [])
        keyword_names = [kw.get('display_name', '') for kw in keywords if kw.get('display_name')]
        row['keywords'] = ';'.join(keyword_names)

        abstract_inverted_index = item.get('abstract_inverted_index')
        row['abstract'] = _parse_abstract_inverted_index(abstract_inverted_index)

        results.append(row)

    return results


def main():
    results_dir = Path(RESULTS_DIR)
    all_data = []

    journal_mapping = _load_journal_mapping()

    for json_file in results_dir.glob('*.json'):
        print(f"Processing {json_file.name}...")
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            data = _extract_data_from_json(json_data, journal_mapping)
            all_data.extend(data)

    output_file = OUTPUT_FILE

    if all_data:
        fieldnames = all_data[0].keys()

        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_data)

        print(f"Successfully created {output_file} with {len(all_data)} records")
    else:
        print("No data found to process")


if __name__ == "__main__":
    main()
