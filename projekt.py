#!/usr/bin/env python
# coding: utf8

import regex 
import codecs
import simplemma
from unidecode import unidecode
import unicodedata
import cze_stemmer
from data import Data
from pymarc import MARCReader, parse_xml_to_array, Record

# todo :
# 1. run program
# 2. import data into memory
# 3. process data (splitting, tekonizations, stemming, indexing)
# 4. read input
# 5. process input
# 6. get data from index
# 7. add some statistics bullshit
# 8. print output

stopWords = []
list_of_data = []
# key = term, value list of doc_id
index = {}
keywords = []

def load_stopwords(language):
    global stopWords
    if language == 'sk':
        with codecs.open('stopwords_sk.txt', encoding='utf-8', mode='r') as f:
            stopWords = [ simplify(line.strip()) for line in f ]
    elif language == 'cz':
        with open('stopwords_cze.txt', encoding='utf-8', mode='r') as f:
            stopWords = [ simplify(line.strip()) for line in f ]

def simplify(text):
	import unicodedata
	try:
		text = unicode(text, 'utf-8')
	except NameError:
		pass
	text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
	return str(text)

def removeStopWords(string):
    string = string.strip()
    string = simplify(string)
    ## remove non-alfanumeric characters
    string = regex.sub('[^A-Za-z0-9 ]+', '', string)
    list_of_terms = regex.split("\s", string)
    list_of_terms = [i for i in list_of_terms if i]
    new_list_of_terms = []
    for term in list_of_terms:
        if term not in stopWords:
            new_list_of_terms.append(term)
        elif len(term) > 3:
            new_list_of_terms.append(term)
    list_of_terms = None
    return new_list_of_terms

def stemming(query, lang):
    if lang == 'sk':
        langdata = simplemma.load_data('sk')
        query = [simplemma.lemmatize(t, langdata) for t in query]
        return query
    elif lang == 'cz':
        stemmedList = []
        for word in query:
            stemmedList.append(cze_stemmer.cz_stem(word))
        return stemmedList

def read_input():
    global keywords
    while True:
        processedQuery = input().strip()
        if processedQuery.startswith("Vrat mi autorov, ktori pisu o :"):
            processedQuery = regex.split(":", processedQuery, 1)[1]
            processedQuery = processedQuery.lower()
            processedQuery = removeStopWords(processedQuery)
            processedQuery = stemming(processedQuery, 'cz')
            keywords = processedQuery
            return getDataObjects(processedQuery)
        else:
            print("wrong format")

def load_documents():
    global list_of_data
    with codecs.open('stud-Cat_short.txt', encoding='utf-8', mode='r') as f:
        data = f.read()
        data = regex.sub('\r\n', '', data)
        data = regex.sub('>  <', '><', data)
    f = codecs.open('demo.txt', encoding='utf-8', mode='a')
    f.write(data)
    f.close()

    data = regex.finditer("tag=\"245\".+?>(<subfield code=\"a\">(?P<title>.+?)</subfield>)?(<subfield code=\"b\">(?P<subtitle>.+?)</subfield>)?(<subfield code=\"c\">(?P<autor>.+?)</subfield>)?(.+?(tag=\"520\".+?>)(<subfield code=\"a\">(?P<abstract>.+?)</subfield>)?)?", data)
    doc_id = 1
    for d in data:
        data_dict = d.groupdict()
        title = data_dict['title']
        if title is None:
            title = ""
        subtitle = data_dict['subtitle']
        if subtitle is None:
            subtitle = ""
        autor = data_dict['autor']
        if autor is None:
            autor = ""
        abstract = data_dict['abstract']
        if abstract is None:
            abstract = ""
        list_of_data.append(Data(ID=doc_id, title=title, subtitle=subtitle, autor=autor, abstract=abstract))
        doc_id += 1

def index_documents():
    global index
    for data in list_of_data:
        merged_data = data.merge.lower()
        merged_data = removeStopWords(merged_data) 
        merged_data = stemming(merged_data, 'cz')
        for term in merged_data:
            if index.get(term) is None:
                postingList = []
                postingList.append(data.ID)
                index[term] = postingList
            elif data.ID not in index[term]:
                index[term].append(data.ID)
    ## iteruj cez data object, ak tam term nie je, pridaj ho + pridaj ID, ak tam term uz je, pridaj iba ID do listu
    #print(index)

def getDataObjects(inputQuery):
    global index
    dataObjects = []
    for term in inputQuery:
        if term in index:
            for data in list_of_data:
                if data.ID in index[term]:
                    dataObjects.append(data)
    return dataObjects

def print_output(dataObjects):
    print("Vstupne keywords: ", keywords)
    if dataObjects:
        for data in dataObjects:
            print(data)
    else:
        print("Nebola najdena zhoda")

def runProgram():
    load_stopwords('cz')
    load_documents()
    index_documents()
    list_of_objects = read_input()
    #analyze()
    print_output(list_of_objects)

# run program
runProgram()