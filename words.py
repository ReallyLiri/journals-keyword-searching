from collections import defaultdict, Counter

import csv
import re
import stanza
from tqdm import tqdm

MIN_WORD_COUNT = 100

STOP_POS = {
    "DET", "ADP", "AUX", "PRON",
    "CCONJ", "SCONJ", "PART", "PUNCT", "SYM"
}

FILTER_WORDS = {
    "pp.",
    "und",
    "al.",
    "no."
}


def should_filter_word(word):
    return word in FILTER_WORDS or len(word) < 3


def should_filter_token(token):
    return token['pos'] in STOP_POS or should_filter_word(token['lemma'])


def get_filtered_tokens(tokens_data):
    return [token for token in tokens_data if not should_filter_token(token)]


def meets_min_count(words, word_counts):
    return all(word_counts[word] >= MIN_WORD_COUNT for word in words)


def clean_html_tags(text):
    return re.sub(r'<[^>]+>', '', str(text or ""))


def process_text_with_stanza(text, nlp):
    if not text or not text.strip():
        return [], []

    doc = nlp(text)
    lemmas = []
    tokens_data = []

    for sentence in doc.sentences:
        for word in sentence.words:
            ner_label = ""
            for entity in sentence.ents:
                if entity.start_char <= word.start_char < entity.end_char:
                    ner_label = entity.type
                    break

            tokens_data.append({
                'text': word.text,
                'lemma': word.lemma.lower(),
                'pos': word.pos,
                'upos': word.upos,
                'ner': ner_label
            })
            lemmas.append(word.lemma.lower())

    return lemmas, tokens_data


def extract_word_pairs(tokens_data, max_distance=5):
    filtered_tokens = get_filtered_tokens(tokens_data)
    pairs = []
    for i, token1 in enumerate(filtered_tokens):
        for j, token2 in enumerate(filtered_tokens[i + 1:], start=i + 1):
            distance = j - i
            if distance <= max_distance:
                pairs.append((token1['lemma'], token2['lemma'], distance))
            else:
                break
    return pairs


def extract_bigrams(tokens_data):
    filtered_tokens = get_filtered_tokens(tokens_data)
    return [(filtered_tokens[i]['lemma'], filtered_tokens[i + 1]['lemma'])
            for i in range(len(filtered_tokens) - 1)]


def extract_trigrams(tokens_data):
    filtered_tokens = get_filtered_tokens(tokens_data)
    return [(filtered_tokens[i]['lemma'], filtered_tokens[i + 1]['lemma'], filtered_tokens[i + 2]['lemma'])
            for i in range(len(filtered_tokens) - 2)]


def main():
    print("Loading Stanza model...")
    nlp = stanza.Pipeline('en', processors='tokenize,pos,lemma,ner', verbose=False)

    print("Reading CSV file...")
    with open('search_results/combined.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        word_counts = Counter()
        word_metadata = {}
        word_pairs = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'distance_sum': 0}))
        bigram_counts = Counter()
        trigram_counts = Counter()

        columns_to_process = ['title', 'keywords', 'abstract']
        columns_for_ngrams = ['title', 'abstract']

        rows = list(reader)

        for row in tqdm(rows, desc="Processing rows"):
            for column in columns_to_process:
                if column in row:
                    text = clean_html_tags(row[column])

                    if text and text.strip():
                        lemmas, tokens_data = process_text_with_stanza(text, nlp)

                        filtered_tokens = get_filtered_tokens(tokens_data)
                        filtered_lemmas = [token['lemma'] for token in filtered_tokens]

                        for token in filtered_tokens:
                            lemma = token['lemma']
                            if lemma not in word_metadata:
                                word_metadata[lemma] = {'upos': [], 'ner': []}

                            if token['upos'] and token['upos'] not in word_metadata[lemma]['upos']:
                                word_metadata[lemma]['upos'].append(token['upos'])

                            if token['ner'] and token['ner'] not in word_metadata[lemma]['ner']:
                                word_metadata[lemma]['ner'].append(token['ner'])

                        word_counts.update(filtered_lemmas)

                        if column in columns_for_ngrams:
                            pairs = extract_word_pairs(tokens_data)
                            for word1, word2, distance in pairs:
                                word_pairs[word1][word2]['count'] += 1
                                word_pairs[word1][word2]['distance_sum'] += distance

                            bigrams = extract_bigrams(tokens_data)
                            bigram_counts.update(bigrams)

                            trigrams = extract_trigrams(tokens_data)
                            trigram_counts.update(trigrams)

    print("Writing words.csv...")
    with open('words.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['word', 'count', 'upos', 'ner'])
        for word, count in word_counts.most_common():
            if count >= MIN_WORD_COUNT:
                metadata = word_metadata.get(word, {'upos': [], 'ner': []})
                upos_str = ';'.join(metadata['upos']) if metadata['upos'] else ''
                ner_str = ';'.join(metadata['ner']) if metadata['ner'] else ''
                writer.writerow([word, count, upos_str, ner_str])

    print("Writing words_graph.csv...")
    with open('words_graph.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['word1', 'word2', 'avg_distance'])
        for word1, connections in word_pairs.items():
            if word_counts[word1] >= MIN_WORD_COUNT:
                for word2, data in connections.items():
                    if word_counts[word2] >= MIN_WORD_COUNT:
                        avg_distance = data['distance_sum'] / data['count']
                        writer.writerow([word1, word2, round(avg_distance, 2)])

    print("Writing words_bigrams.csv...")
    with open('words_bigrams.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['word1', 'word2', 'count'])
        for bigram, count in bigram_counts.most_common():
            if meets_min_count(bigram, word_counts):
                writer.writerow([*bigram, count])

    print("Writing words_trigrams.csv...")
    with open('words_trigrams.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['word1', 'word2', 'word3', 'count'])
        for trigram, count in trigram_counts.most_common():
            if meets_min_count(trigram, word_counts):
                writer.writerow([*trigram, count])

    print("Done!")
    print(f"Processed {len(word_counts)} unique words")
    print(f"Found {sum(len(connections) for connections in word_pairs.values())} word pairs")
    print(f"Found {len(bigram_counts)} bigrams")
    print(f"Found {len(trigram_counts)} trigrams")


if __name__ == "__main__":
    main()
