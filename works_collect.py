import csv
import json
from pathlib import Path

import requests

RESULTS_DIR = 'search_results'
OUTPUT_FILE = 'works.csv'
JOURNALS_FILE = 'journals.csv'
VERBOSE = True
FOLLOW_DOIS = True



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


def _follow_doi_redirects(doi):
    if not doi:
        return ""

    url = doi
    if not url.startswith('http'):
        url = f'https://doi.org/{doi.replace("https://doi.org/", "")}'

    try:
        response = requests.head(url, allow_redirects=True, timeout=(1.5, 1.5))
        return response.url
    except requests.exceptions.Timeout:
        if VERBOSE:
            print(f"Timeout following DOI {doi}, returning empty")
        return ""
    except Exception as e:
        print(f"Error following DOI {doi}: {e}")
        raise


def _check_pdf_exists(work_id):
    pdf_path = Path(f'pdfs/works/{work_id}.txt')
    return pdf_path.exists()


def _load_existing_doi_follows():
    existing_follows = {}
    if Path(OUTPUT_FILE).exists():
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if 'doi_follow' in reader.fieldnames and 'id' in reader.fieldnames and 'doi' in reader.fieldnames:
                for row in reader:
                    work_id = row.get('id', '')
                    doi = row.get('doi', '')
                    doi_follow = row.get('doi_follow', '')
                    if work_id and doi_follow and doi_follow != doi:
                        existing_follows[work_id] = doi_follow
    return existing_follows


def _load_journal_mapping():
    journal_mapping = {}
    with open(JOURNALS_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_id = row['OpenAlexSourceId']
            journal_mapping[source_id] = row
    return journal_mapping


def _extract_data_from_json(json_data, journal_mapping, existing_doi_follows):
    results = []

    for i, item in enumerate(json_data):
        if VERBOSE:
            print(f"  Processing item {i + 1}/{len(json_data)}")
        row = {}

        openalex_prefix = 'https://openalex.org/'
        row['id'] = item.get('id', '').replace(openalex_prefix, '')
        row['doi'] = item.get('doi', '')
        if row['id'] in existing_doi_follows:
            row['doi_follow'] = existing_doi_follows[row['id']]
        else:
            row['doi_follow'] = _follow_doi_redirects(row['doi']) if FOLLOW_DOIS else ""
        row['has_pdf'] = _check_pdf_exists(row['id'])
        row['title'] = item.get('title', '')
        row['publication_date'] = item.get('publication_date', '')

        primary_location = item.get('primary_location', {})
        source = primary_location.get('source', {}) if primary_location else {}
        row['source_id'] = source.get('id', '').replace(openalex_prefix, '') if source else ''

        journal = journal_mapping.get(row['source_id'], {})
        row['journal_name'] = journal.get('Journal Name', '')
        row['journal_category'] = journal.get('Category', '')

        journal_url = journal.get('URL', '')
        row['jstor'] = 'jstor.org' in journal_url.lower()
        row['tandf'] = 'tandfonline.com' in journal_url.lower()

        open_access = item.get('open_access', {})
        row['oa_status'] = open_access.get('oa_status', '') if open_access else ''
        row['oa_url'] = open_access.get('oa_url', '') if open_access else ''

        authors = item.get('authorships', [])
        author_names = [author.get('raw_author_name') for author in authors if author.get('raw_author_name')]
        author_ids = [author.get('author', {}).get('id', '').replace(openalex_prefix, '') for author in authors if author.get('author', {}).get('id')]
        row['authors'] = ';'.join(author_names)
        row['author_ids'] = ';'.join(author_ids)

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
    existing_doi_follows = _load_existing_doi_follows()

    json_files = list(results_dir.glob('*.json'))
    for i, json_file in enumerate(json_files):
        print(f"Processing {json_file.name} ({i + 1}/{len(json_files)})...")
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            data = _extract_data_from_json(json_data, journal_mapping, existing_doi_follows)
            all_data.extend(data)

    output_file = OUTPUT_FILE

    if all_data:
        fieldnames = all_data[0].keys()

        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(all_data)

        print(f"Successfully created {output_file} with {len(all_data)} records")
    else:
        print("No data found to process")


if __name__ == "__main__":
    main()
