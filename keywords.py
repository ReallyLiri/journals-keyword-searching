import stanza
import re
from collections import defaultdict, Counter
import csv
from tqdm import tqdm

MIN_WORD_COUNT = 100


def clean_html_tags(text):
    if not text or text == "":
        return ""
    return re.sub(r'<[^>]+>', '', str(text))


def process_text_with_stanza(text, nlp):
    if not text or text.strip() == "":
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
    STOP_POS = {
        "DET", "ADP", "AUX", "PRON",
        "CCONJ", "SCONJ", "PART"
    }

    filtered_tokens = [token for token in tokens_data if token['pos'] not in STOP_POS]

    pairs = []
    for i, token1 in enumerate(filtered_tokens):
        for j, token2 in enumerate(filtered_tokens[i + 1:], start=i + 1):
            distance = j - i
            if distance <= max_distance:
                pairs.append((token1['lemma'], token2['lemma'], distance))
            else:
                break

    return pairs


def main():
    STOP_POS = {
        "DET", "ADP", "AUX", "PRON",
        "CCONJ", "SCONJ", "PART"
    }

    print("Loading Stanza model...")
    nlp = stanza.Pipeline('en', processors='tokenize,pos,lemma,ner', verbose=False)

    print("Reading CSV file...")
    with open('search_results/combined.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        word_counts = Counter()
        word_metadata = {}
        word_pairs = defaultdict(lambda: defaultdict(int))

        columns_to_process = ['title', 'keywords', 'abstract']

        rows = list(reader)

        for row in tqdm(rows, desc="Processing rows"):
            for column in columns_to_process:
                if column in row:
                    text = clean_html_tags(row[column])

                    if text and text.strip():
                        lemmas, tokens_data = process_text_with_stanza(text, nlp)

                        filtered_lemmas = [
                            token['lemma'] for token in tokens_data
                            if token['pos'] not in STOP_POS
                        ]

                        for token in tokens_data:
                            if token['pos'] not in STOP_POS:
                                lemma = token['lemma']
                                if lemma not in word_metadata:
                                    word_metadata[lemma] = {
                                        'upos': token['upos'],
                                        'ner': token['ner']
                                    }

                        word_counts.update(filtered_lemmas)

                        pairs = extract_word_pairs(tokens_data)
                        for word1, word2, distance in pairs:
                            word_pairs[word1][word2] += 1

    print("Writing words.csv...")
    with open('words.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['word', 'count', 'upos', 'ner'])
        for word, count in word_counts.most_common():
            if count >= MIN_WORD_COUNT:
                metadata = word_metadata.get(word, {'upos': '', 'ner': ''})
                writer.writerow([word, count, metadata['upos'], metadata['ner']])

    print("Writing words_graph.csv...")
    with open('words_graph.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['word1', 'word2', 'distance'])
        for word1, connections in word_pairs.items():
            if word_counts[word1] >= MIN_WORD_COUNT:
                for word2, count in connections.items():
                    if word_counts[word2] >= MIN_WORD_COUNT:
                        writer.writerow([word1, word2, count])

    print("Done!")
    print(f"Processed {len(word_counts)} unique words")
    print(f"Found {sum(len(connections) for connections in word_pairs.values())} word pairs")


if __name__ == "__main__":
    main()
