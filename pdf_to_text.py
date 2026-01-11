#!/usr/bin/env python3

import os
import sys
from pathlib import Path
from pdfminer.high_level import extract_text
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def pdf_to_text(pdf_path, output_path):
    try:
        text = extract_text(pdf_path)

        lines = text.split('\n')
        processed_lines = []
        i = 0

        while i < len(lines):
            line = lines[i]

            while i + 1 < len(lines) and line.rstrip().endswith('-'):
                line = line.rstrip()[:-1] + lines[i + 1].lstrip()
                i += 1

            processed_lines.append(line)
            i += 1

        processed_text = '\n'.join(processed_lines)

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(processed_text)

        return True

    except Exception as e:
        return f"Error processing {pdf_path}: {e}"

def process_directory(directory):
    dir_path = Path(directory)

    if not dir_path.exists():
        print(f"Directory {directory} does not exist", file=sys.stderr)
        sys.exit(1)

    pdf_files = list(dir_path.rglob("*.pdf"))

    if not pdf_files:
        print(f"No PDF files found in {directory}")
        return

    successful = 0
    failed = 0
    errors = []

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(pdf_to_text, pdf_file, pdf_file.with_suffix('.txt')): pdf_file
            for pdf_file in pdf_files
        }

        with tqdm(total=len(pdf_files), desc="Converting PDFs") as pbar:
            for future in as_completed(futures):
                pdf_file = futures[future]
                result = future.result()

                if result is True:
                    successful += 1
                else:
                    failed += 1
                    errors.append(result)

                pbar.update(1)

    print(f"\nCompleted: {successful} successful, {failed} failed")

    if errors:
        print("\nErrors:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)

def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <directory>", file=sys.stderr)
        sys.exit(1)

    directory = sys.argv[1]
    process_directory(directory)

if __name__ == "__main__":
    main()