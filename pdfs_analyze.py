import csv
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import re
import sys
from wordcloud import WordCloud
from collections import Counter

PDFS_DIR = 'pdfs'
CSV_FILENAME = 'index.csv'
SEARCH_WORD = 'israel'
COLUMN_NAME = 'israel_count'


def count_israel_in_text(row, base_dir):
    if 'ID' in row:
        file_id = row['ID']
    else:
        url = row.get('url', '')
        if url and 'jstor.org/stable/' in url:
            file_id = url.split('jstor.org/stable/')[-1]
            if '/' in file_id:
                file_id = file_id.split('/')[-1]
        else:
            file_id = row.get('citation_key', '')

    txt_file = base_dir / f"{file_id}.txt"

    if not txt_file.exists():
        return None, None, None

    try:
        with open(txt_file, 'r', encoding='utf-8') as f:
            text = f.read()
    except Exception:
        with open(txt_file, 'r', encoding='latin-1') as f:
            text = f.read()

    pattern = re.compile(rf'\b{SEARCH_WORD}\b', re.IGNORECASE)
    matches = pattern.findall(text)
    return len(matches), text, file_id


def process_csv_and_generate_wordclouds(csv_path):
    try:
        base_dir = csv_path.parent
        all_texts = []
        regenerate_combined = False

        dir_name_words = set(re.findall(r'\b[a-zA-Z]+\b', base_dir.name.lower()))

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames

        if COLUMN_NAME not in fieldnames:
            fieldnames = list(fieldnames) + [COLUMN_NAME]

        for row in rows:
            count, text, file_id = count_israel_in_text(row, base_dir)
            row[COLUMN_NAME] = '' if count is None else str(count)

            if text:
                output_file = base_dir / f'{file_id}_wordcloud.png'
                if not output_file.exists():
                    generate_wordcloud(text, output_file, dir_name_words)
                    regenerate_combined = True
                all_texts.append(text)

        with open(csv_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        combined_output_file = base_dir / 'wordcloud.png'
        if all_texts and (regenerate_combined or not combined_output_file.exists()):
            combined_text = ' '.join(all_texts)
            generate_wordcloud(combined_text, combined_output_file, dir_name_words)

        return csv_path, len(rows), None
    except Exception as e:
        return csv_path, 0, str(e)


def load_stopwords():
    stopwords_file = Path('stopwords-en.txt')
    if stopwords_file.exists():
        with open(stopwords_file, 'r', encoding='utf-8') as f:
            return set(line.strip().lower() for line in f if line.strip())
    return set()


def generate_wordcloud(text, output_path, additional_stopwords=None):
    stopwords = load_stopwords()
    if additional_stopwords:
        stopwords = stopwords.union(additional_stopwords)

    words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
    filtered_words = [word for word in words if word not in stopwords]

    if not filtered_words:
        return False

    word_freq = Counter(filtered_words)
    top_words = dict(word_freq.most_common(100))

    if not top_words:
        return False

    wc = WordCloud(width=1600, height=800, background_color='white',
                   max_words=100, relative_scaling=0.5,
                   colormap='viridis').generate_from_frequencies(top_words)

    wc.to_file(str(output_path))
    return True


def main():
    pdfs_dir = Path(PDFS_DIR)
    csv_files = list(pdfs_dir.glob(f'**/{CSV_FILENAME}'))

    if not csv_files:
        print(f"No {CSV_FILENAME} files found")
        sys.exit(1)

    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_csv_and_generate_wordclouds, csv_path): csv_path
                   for csv_path in csv_files}

        with tqdm(total=len(csv_files), desc="Processing files and generating wordclouds") as pbar:
            for future in as_completed(futures):
                csv_path, rows_processed, error = future.result()

                if error:
                    print(f"\nError processing {csv_path}: {error}")
                    executor.shutdown(wait=False, cancel_futures=True)
                    sys.exit(1)

                pbar.update(1)

    print(f"\nSuccessfully processed {len(csv_files)} directories")


if __name__ == "__main__":
    main()
