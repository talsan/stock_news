from collections import Counter
import re
import pandas as pd

def most_common_words_in_list(names_list):
    names_str = ' '.join(names_list)
    name_count = Counter(names_str.split(' '))
    return name_count

def get_stop_words():
    return ['inc', 'corp', 'class', 'a', 'ltd', 'plc','b','c','co','nv','i','clas',]

def clean_name(name):
    name_cln = re.sub('[^\w\s]','', name.lower())
    for word in get_stop_words():
        name_cln = re.sub(f'\\b{word.lower()}\\b', '', name_cln)
    name_cln = re.sub('\\s+', ' ', name_cln).strip()
    return(name_cln)

def get_credible_sources(source_file):
    return(pd.read_csv(source_file)['website'].to_list())

def convert_sources_csv_to_query(source_file):
    credible_sources = pd.read_csv(source_file)['website'].to_list()
    credible_sources_filter = 'pageUrl:or(' + ','.join([f'"{source}"' for source in credible_sources]) + ')'