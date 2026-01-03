import os
import traceback
import csv

import httpx
import ollama

MODEL_NAME = "llama3"
INPUT_CSV = "search_results/combined.csv"
SENTIMENT_SPECIFIC_RESULTS_DIR = "sentiment_specific_results"
SENTIMENT_THEMES_RESULTS_DIR = "sentiment_themes_results"

THEMES = [
    "colonialism", "settler colonialism", "apartheid", "genocide", "fascism",
    "occupation", "oppression", "resistance", "war crimes", "ethnic cleansing",
    "immigration", "migration", "refugee", "innovation", "democracy",
    "nation building", "liberalism", "peace", "tolerance", "agency",
    "egalitarian", "ethno-national", "nazism", "holocaust", "religious-zionist",
    "supremacy", "racism", "Nakba", "Zionism", "Zionist", "Messianism",
    "messianic", "Whole-Israel", "West Bank", "antisemitism", "anti-Zionism",
    "diaspora", "Islamophobia"
]

OLLAMA_URL = os.environ.get("OLLAMA_URL")

client = ollama.Client(
    host=OLLAMA_URL,
    timeout=httpx.Timeout(connect=60.0, read=None, write=None, pool=None)
)

os.makedirs(SENTIMENT_SPECIFIC_RESULTS_DIR, exist_ok=True)
os.makedirs(SENTIMENT_THEMES_RESULTS_DIR, exist_ok=True)


def run_prompt(prompt):
    try:
        messages = [
            {
                'role': 'user',
                'content': prompt
            }
        ]

        response = client.chat(
            model=MODEL_NAME,
            messages=messages,
            stream=False,
            options={
                "num_predict": 4096
            }
        )

        result = response['message']['content']
        return result

    except ollama.ResponseError as e:
        traceback.print_exc()
        print(f"\nOllama API Error: {e}")
        raise e
    except Exception as e:
        traceback.print_exc()
        print(f"\nAn unexpected error occurred: {e}")
        raise e


def clean_json_result(result, row_id):
    cleaned_result = result.strip()
    if cleaned_result.startswith('```'):
        lines = cleaned_result.split('\n')
        if lines[0].startswith('```'):
            lines = lines[1:]
        if lines and lines[-1].strip() == '```':
            lines = lines[:-1]
        cleaned_result = '\n'.join(lines)

    if '{' not in cleaned_result:
        print(f"Error: No opening brace found in result for {row_id}")
        return None

    cleaned_result = cleaned_result[cleaned_result.index('{'):]

    if '}' not in cleaned_result:
        cleaned_result += '"}'
    else:
        cleaned_result = cleaned_result[:cleaned_result.rindex('}') + 1]

    return cleaned_result


def build_content_section(abstract, keywords):
    if abstract:
        return f"Content: {abstract}"
    else:
        keyword_list = keywords.replace(';', ', ')
        return f"Academic Keywords: {keyword_list}"


def analyze_sentiment_for_row(row_id, title, authors, content_section):
    output_path = f'{SENTIMENT_SPECIFIC_RESULTS_DIR}/{row_id}.json'

    if os.path.exists(output_path):
        return True

    prompt = f"""Analyze the sentiment towards "Israel" in this academic text.

Title: {title}
Authors: {authors}
{content_section}

Output ONLY the following JSON format with no additional text:
{{
  "sentiment": "positive" or "negative" or "neutral" or "unclassified",
  "reason": "your explanation here"
}}

Use "unclassified" if there is not enough information to determine sentiment."""

    print(f"\nAnalyzing sentiment: '{title}'")
    result = run_prompt(prompt)

    if result:
        cleaned_result = clean_json_result(result, row_id)
        if cleaned_result:
            with open(output_path, 'w', encoding='utf-8') as out_f:
                out_f.write(cleaned_result)
            return True

    return False


def analyze_themes_for_row(row_id, title, authors, content_section):
    output_path = f'{SENTIMENT_THEMES_RESULTS_DIR}/{row_id}.json'

    if os.path.exists(output_path):
        return True

    themes_list = ', '.join(THEMES)
    prompt = f"""Identify which themes from the following list this academic text may be addressing.

Title: {title}
Authors: {authors}
{content_section}

Themes: {themes_list}

Output ONLY a JSON with a "themes" field containing a list of relevant themes from the input list:
{{
  "themes": ["theme1", "theme2", ...]
}}

Only include themes that are actually related to the text. If no themes apply, return an empty list."""

    print(f"\nAnalyzing themes: '{title}'")
    result = run_prompt(prompt)

    if result:
        cleaned_result = clean_json_result(result, row_id)
        if cleaned_result:
            with open(output_path, 'w', encoding='utf-8') as out_f:
                out_f.write(cleaned_result)
            return True

    return False


def analyze_sentiments():
    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        i = 0
        for row in reader:
            i += 1
            row_id = row['id']
            title = row['title']
            abstract = row['abstract']
            keywords = row['keywords']
            authors = row['authors'].replace(';', ', ')

            content_section = build_content_section(abstract, keywords)

            sentiment_done = analyze_sentiment_for_row(row_id, title, authors, content_section)
            themes_done = analyze_themes_for_row(row_id, title, authors, content_section)

            if sentiment_done and themes_done:
                print(f"Completed #{i}: {row_id}")


if __name__ == "__main__":
    analyze_sentiments()
