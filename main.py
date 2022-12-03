import requests
from tqdm import tqdm
import os
from lm_dataformat import Archive
import shutil
import spacy
import json
import glob

url = 'https://wolnelektury.pl/api/books/'
urls = []

#api returns all books metadata collection with hrefs to each book's further details. 
#querying endpoint for each book's media urls details is time consuming. Urls are synthetized using a pattern.

response = requests.get(url)
if response.ok:
    
    #urls  = [ requests.get(book['href']).json()['txt'] for book in response.json()] 
    
    urls = [ book['href'].replace('/api/books/','/media/book/txt/')[:-1]+'.txt' for book in response.json() ]
    
    

def download_file(url):

    ok = True
    file_name = './downloaded.txt'
    txt = ''

    response = requests.get(url, stream=True)
    total_size_in_bytes = int(response.headers.get('content-length', 0))
    block_size = 1024
    progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
    with open(file_name, 'wb') as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()
    if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
        ok = False

    if ok:
      with open(file_name, encoding='utf8') as f:
        txt = f.read()
        #Each text has a similiar disclaimer appended. 
        #Attempting to partition output text using a pattern to remove discalimer.
        txt = txt.partition('\n-----\nTa lektura,')[0]
        
    return ok, txt

def get_word_stats(txt):
    if not txt:
        return 0, 0, 0, 0, 0, 0

    sentences = 0
    words = 0
    verbs = 0
    nouns = 0
    punctuations = 0
    symbols = 0

    doc = nlp(txt)

    sentences = len(list(doc.sents))
    words = len([token.text for token in doc if not token.is_punct])
    nouns = len([token.text for token in doc if (not token.is_stop and not token.is_punct and token.pos_ == "NOUN")])
    verbs = len([token.text for token in doc if (not token.is_stop and not token.is_punct and token.pos_ == "VERB")])
    punctuations = len([token.text for token in doc if (token.is_punct or token.pos_ == "PUNCT")])
    symbols = len([token.text for token in doc if (token.pos_ == "SYM")])

    return sentences, words, verbs, nouns, punctuations, symbols

ar = Archive('./data')

file_name_zst = './wolne_lektury_corpus.jsonl.zst'
file_name_manifest = './wolne_lektury_corpus.manifest'

#disabling some unused model features speeds things up to 20%
nlp = spacy.load("pl_core_news_md", disable=('ner','lemmatizer','textcat','entity_linker'))

total_len = 0
total_docs = 0
total_sentences = 0
total_words = 0
total_verbs = 0
total_nouns = 0
total_punctuations = 0
total_symbols = 0

for idx, url_txt in enumerate(urls):
    print(url_txt)
    ok, txt = download_file(url_txt)
    if ok:
      l = len(txt.strip())
      if l > 100000:
        nlp.max_length = len(txt) + 100
      sentences, words, verbs, nouns, punctuations, symbols = get_word_stats(txt.strip())
      total_words += words
      total_verbs += verbs
      total_nouns += nouns
      total_len += l
      total_docs += 1
      total_sentences += sentences
      total_punctuations += punctuations
      total_symbols += symbols
      meta = {'url' : url_txt, 'length': l, 'sentences': sentences, 'words': words, 'verbs': verbs, 'nouns': nouns, 'punctuations': punctuations, 'symbols': symbols}
      ar.add_data(txt.strip(), meta = meta)
      print("Added {num}/{total} ".format(num=idx+1, total=len(urls)) + meta.get('url'))

ar.commit()


data_files= glob.glob('./data/*')
file_size = 0

#This solves an issue where data_files remained locked after ar commiting, causing error on cleanup
ar = None

for f in data_files:
    if f.endswith('.zst'):
        shutil.copy(f, os.path.join(file_name_zst))
        file_size = os.path.getsize(file_name_zst)

    os.remove(f)

manifest = {"project" : "SpeakLeash", "name": "wolne_lektury_corpus", "description": "6k+ school readings collection, WOLNELEKTURY.PL", "license": "Public Domain; Creative Commons Attribution-ShareAlike 3.0; Free Art License 1.3", "language": "pl", "file_size" : file_size, "sources": [{"name": "wolne_lektury_corpus", "url": "https://wolnelektury.pl/api/books/", "license": "Public Domain; Creative Commons Attribution-ShareAlike 3.0; Free Art License 1.3"}], "stats": {"documents": total_docs, "sentences": total_sentences, "words" : total_words, "nouns" : total_nouns, "verbs" : total_verbs, "characters": total_len, "punctuations" : total_punctuations, "symbols" : total_symbols}}
json_manifest = json.dumps(manifest, indent = 4) 

with open(file_name_manifest, 'w') as mf:
    mf.write(json_manifest)