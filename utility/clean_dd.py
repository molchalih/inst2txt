import re

input_file = 'utility/dd.txt'
output_file = 'utility/dd_cleaned.txt'

with open(input_file, 'r', encoding='utf-8') as f:
    lines = f.readlines()

cleaned_lines = []
for line in lines:
    url = line.strip()
    if not url:
        continue  # skip empty lines
    # Remove URL parameters (anything after '?')
    url = re.sub(r'\?.*$', '', url)
    cleaned_lines.append(url)

with open(output_file, 'w', encoding='utf-8') as f:
    for url in cleaned_lines:
        f.write(url + '\n') 