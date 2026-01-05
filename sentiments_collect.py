import os
import json
import csv

SENTIMENT_SPECIFIC_DIR = "sentiment_specific_results"
SENTIMENT_THEMES_DIR = "sentiment_themes_results"
OUTPUT_CSV = "sentiments.csv"


def load_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in file {file_path}: {e}")
    except Exception as e:
        raise RuntimeError(f"Error reading file {file_path}: {e}")


def collect_sentiments():
    sentiment_files = {}
    for file_name in os.listdir(SENTIMENT_SPECIFIC_DIR):
        if file_name.endswith('.json'):
            file_id = file_name[:-5]
            file_path = os.path.join(SENTIMENT_SPECIFIC_DIR, file_name)
            sentiment_files[file_id] = load_json_file(file_path)

    themes_files = {}
    for file_name in os.listdir(SENTIMENT_THEMES_DIR):
        if file_name.endswith('.json'):
            file_id = file_name[:-5]
            file_path = os.path.join(SENTIMENT_THEMES_DIR, file_name)
            themes_files[file_id] = load_json_file(file_path)

    all_ids = set(sentiment_files.keys()) | set(themes_files.keys())

    rows = []
    for file_id in sorted(all_ids):
        sentiment_data = sentiment_files.get(file_id, {})
        themes_data = themes_files.get(file_id, {})

        sentiment = sentiment_data.get('sentiment', '')
        sentiment_reason = sentiment_data.get('reason', '')
        themes = themes_data.get('themes', [])

        themes_str = ', '.join(themes) if isinstance(themes, list) else str(themes)

        rows.append({
            'id': file_id,
            'sentiment': sentiment,
            'sentiment_reason': sentiment_reason,
            'themes': themes_str
        })

    with open(OUTPUT_CSV, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['id', 'sentiment', 'sentiment_reason', 'themes']
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Successfully collected {len(rows)} records to {OUTPUT_CSV}")


if __name__ == "__main__":
    try:
        collect_sentiments()
    except Exception as e:
        print(f"Error: {e}")
        raise
