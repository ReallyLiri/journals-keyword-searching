from collections import defaultdict, Counter

import csv
import re
import stanza
from tqdm import tqdm

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


def clean_html_tags(text):
    return re.sub(r'<[^>]+>', '', str(text or ""))


def process_text_with_stanza(text, nlp):
    if not text or not text.strip():
        return [], []

    doc = nlp(text)
    lemmas = []
    tokens_data = []

    for sent_idx, sentence in enumerate(doc.sentences):
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
                'ner': ner_label,
                'sentence_id': sent_idx
            })
            lemmas.append(word.lemma.lower())

    return lemmas, tokens_data


def extract_word_pairs(tokens_data):
    filtered_tokens = get_filtered_tokens(tokens_data)
    pairs = []
    for i, token1 in enumerate(filtered_tokens):
        for j, token2 in enumerate(filtered_tokens[i + 1:], start=i + 1):
            if token1['sentence_id'] != token2['sentence_id']:
                break
            distance = j - i
            pairs.append((token1['lemma'], token2['lemma'], distance))
    return pairs


def extract_bigrams(tokens_data):
    filtered_tokens = get_filtered_tokens(tokens_data)
    return [(filtered_tokens[i]['lemma'], filtered_tokens[i + 1]['lemma'])
            for i in range(len(filtered_tokens) - 1)]


def extract_trigrams(tokens_data):
    filtered_tokens = get_filtered_tokens(tokens_data)
    return [(filtered_tokens[i]['lemma'], filtered_tokens[i + 1]['lemma'], filtered_tokens[i + 2]['lemma'])
            for i in range(len(filtered_tokens) - 2)]


def process_work(work_id, row, nlp, columns_to_process, columns_for_ngrams):
    work_data = {
        'work_id': work_id,
        'word_counts': Counter(),
        'word_metadata': {},
        'word_pairs': defaultdict(lambda: defaultdict(lambda: {'count': 0, 'distance_sum': 0})),
        'bigram_counts': Counter(),
        'trigram_counts': Counter()
    }

    for column in columns_to_process:
        if column in row:
            text = clean_html_tags(row[column])

            if text and text.strip():
                lemmas, tokens_data = process_text_with_stanza(text, nlp)

                filtered_tokens = get_filtered_tokens(tokens_data)
                filtered_lemmas = [token['lemma'] for token in filtered_tokens]

                for token in filtered_tokens:
                    lemma = token['lemma']
                    if lemma not in work_data['word_metadata']:
                        work_data['word_metadata'][lemma] = {'upos': [], 'ner': []}

                    if token['upos'] and token['upos'] not in work_data['word_metadata'][lemma]['upos']:
                        work_data['word_metadata'][lemma]['upos'].append(token['upos'])

                    if token['ner'] and token['ner'] not in work_data['word_metadata'][lemma]['ner']:
                        work_data['word_metadata'][lemma]['ner'].append(token['ner'])

                work_data['word_counts'].update(filtered_lemmas)

                if column in columns_for_ngrams:
                    pairs = extract_word_pairs(tokens_data)
                    for word1, word2, distance in pairs:
                        work_data['word_pairs'][word1][word2]['count'] += 1
                        work_data['word_pairs'][word1][word2]['distance_sum'] += distance

                    bigrams = extract_bigrams(tokens_data)
                    work_data['bigram_counts'].update(bigrams)

                    trigrams = extract_trigrams(tokens_data)
                    work_data['trigram_counts'].update(trigrams)

    return work_data


def main():
    print("Loading Stanza model...")
    nlp = stanza.Pipeline('en', processors='tokenize,pos,lemma,ner', verbose=False)

    print("Reading CSV file...")
    with open('search_results/combined.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        columns_to_process = ['title', 'keywords', 'abstract']
        columns_for_ngrams = ['title', 'abstract']

        rows = list(reader)

        words_data = []
        graph_data = []
        bigrams_data = []
        trigrams_data = []

        for row in tqdm(rows, desc="Processing works"):
            work_id = row.get('id', '')
            if not work_id:
                continue

            work_data = process_work(work_id, row, nlp, columns_to_process, columns_for_ngrams)

            for word, count in work_data['word_counts'].items():
                metadata = work_data['word_metadata'].get(word, {'upos': [], 'ner': []})
                upos_str = ';'.join(metadata['upos']) if metadata['upos'] else ''
                ner_str = ';'.join(metadata['ner']) if metadata['ner'] else ''
                words_data.append([work_id, word, count, upos_str, ner_str])

            for word1, connections in work_data['word_pairs'].items():
                for word2, data in connections.items():
                    if data['count'] > 0:
                        avg_distance = data['distance_sum'] / data['count']
                        graph_data.append([work_id, word1, word2, round(avg_distance, 2), data['count']])

            for bigram, count in work_data['bigram_counts'].items():
                bigrams_data.append([work_id, bigram[0], bigram[1], count])

            for trigram, count in work_data['trigram_counts'].items():
                trigrams_data.append([work_id, trigram[0], trigram[1], trigram[2], count])

    print("Writing words.csv...")
    with open('words.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(['work_id', 'word', 'count', 'part_of_speech', 'named_entity'])
        writer.writerows(words_data)

    print("Writing words_graph.csv...")
    with open('words_graph.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(['work_id', 'word1', 'word2', 'avg_distance', 'pair_count'])
        writer.writerows(graph_data)

    print("Writing words_bigrams.csv...")
    with open('words_bigrams.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(['work_id', 'word1', 'word2', 'count'])
        writer.writerows(bigrams_data)

    print("Writing words_trigrams.csv...")
    with open('words_trigrams.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(['work_id', 'word1', 'word2', 'word3', 'count'])
        writer.writerows(trigrams_data)

    print("Done!")
    print(f"Processed {len(rows)} works")
    print(f"Generated {len(words_data)} word entries")
    print(f"Generated {len(graph_data)} word pair entries")
    print(f"Generated {len(bigrams_data)} bigram entries")
    print(f"Generated {len(trigrams_data)} trigram entries")


if __name__ == "__main__":
    main()
