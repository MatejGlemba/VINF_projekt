#!/usr/bin/env python
# coding: utf8

import regex 
import codecs
import math
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

expectedInputKeyWords = ['autor', 'titul', 'abstrakt']
actualInputKeyWords = []
stopWords = []
list_of_data = []
# key = term, value list of doc_id
index = {}
collectionFreqIndex = {}
documentFreqIndex = {}
inverseDocumentFreqIndex = {}
keywords = []

def load_stopwords(language):
    global stopWords
    if language == 'sk':
        with codecs.open('stopwords_sk.txt', encoding='utf-8', mode='r') as f:
            stopWords = [ removeDiacritics(line.strip()) for line in f ]
    elif language == 'cz':
        with open('stopwords_cze.txt', encoding='utf-8', mode='r') as f:
            stopWords = [ removeDiacritics(line.strip()) for line in f ]

def removeDiacritics(text):
	import unicodedata
	try:
		text = unicode(text, 'utf-8')
	except NameError:
		pass
	text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
	return str(text)

def removeStopWords(string):
    string = string.strip()
    string = removeDiacritics(string)
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
    global actualInputKeyWords
    print("---------------------------------------")
    print("Input format : [Co hladam]:[Podla coho]")
    while True:
        query = input().strip()
        if regex.match(".+:.+", query):
            splitInput = regex.split(":", query, 1)
            #input keys
            inputKeys = splitInput[0].lower()
            inputKeys = removeStopWords(inputKeys)
            inputKeys = stemming(inputKeys, 'cz')
            actualInputKeyWords = inputKeys
            #print("inputKeys ", actualInputKeyWords)
            #query
            query = splitInput[1]
            query = query.lower()
            query = removeStopWords(query)
            query = stemming(query, 'cz')
            #print(query)
            keywords = query
            #print("query", keywords)
            return getDataObjects(query)
        else:
            return

def load_documents():
    global list_of_data
    with codecs.open('stud-Cat_short.txt', encoding='utf-8', mode='r') as f:
        data = f.read()
        data = regex.sub('\r\n', '', data)
        data = regex.sub('>  <', '><', data)
        #data = regex.sub('><record>', '>\n<record>', data)
    #fw = codecs.open('demo.txt', encoding='utf-8', mode='a')
    #fw.write(data) 
    #fw.close()

    data = regex.finditer("<datafield tag=\"245\" ind1=\"[0-9 ]\" ind2=\"[0-9 ]\">(<subfield code=\"a\">(?P<title>.+?)</subfield>)?(<subfield code=\"b\">(?P<subtitle>.+?)</subfield>)?(<subfield code=\"c\">(?P<autor>.+?)</subfield>)?(.+?<datafield tag=\"520\" ind1=\"[0-9 ]\" ind2=\"[0-9 ]\">(<subfield code=\"a\">(?P<abstract>.+?)</subfield>)?)?", data)
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

def processData(data):
    merged_data = data.merge.lower()
    merged_data = removeStopWords(merged_data) 
    merged_data = stemming(merged_data, 'cz')
    return merged_data

def index_documents():
    ## index = {
    #  term : [ {data.id : freq}, {data.id} : freq, ....]
    #  term : [ {data.id : freq}, {data.id} : freq, ....]
    # }
    global index
    global collectionFreqIndex
    global documentFreqIndex
    global inverseDocumentFreqIndex

    for data in list_of_data:
        merged_data = processData(data)
        for term in merged_data:
            if index.get(term) is None:
                postingList = []
                docIdFreqDict = {}
                docIdFreqDict[data.ID] = 1
                postingList.append(docIdFreqDict)
                index[term] = postingList
            update = False
            for tupl in index.get(term):
                if data.ID in tupl:
                    freq = tupl[data.ID]
                    tupl[data.ID] = freq + 1
                    update = True
                    break
            if not update:
                docIdFreqDict = {}
                docIdFreqDict[data.ID] = 1
                index[term].append(docIdFreqDict)
    ## Fill document and collection frequency index
    for k,postingList in index.items():
        documentFreqIndex[k] = len(postingList)
        for tmp in postingList:
            for freq in tmp.values():
                if k not in collectionFreqIndex:
                    collectionFreqIndex[k] = freq
                else:
                    collectionFreqIndex[k] += freq

    inverseDocumentFreqIndex = {}
    for k,v in documentFreqIndex.items():
        inverseDocumentFreqIndex[k] = math.log(len(list_of_data)/v)
    
    #print("INVDOCFRQ" , dict(sorted(inverseDocumentFreqIndex.items(), key=lambda item: item[1])))
    #print(index)
    #print("DOCFRQ" , dict(sorted(documentFreqIndex.items(), key=lambda item: item[1], reverse=True)))
    #print("COLLFRQ" , dict(sorted(collectionFreqIndex.items(), key=lambda item: item[1], reverse=True)))
    #print("INDEX", dict(sorted(index.items(), key=lambda item: len(item[1]), reverse=True)))

def printIndex(num):
    global index
    #print(index)
    index = dict(sorted(index.items(), key=lambda item: len(item[1]), reverse=True))
    n = 0
    for k,v in index.items():
        print(k, v)
        n += 1
        if n > num:
            break

def getDataObjects(inputQuery):
    global index
    global inverseDocumentFreqIndex
    dataObjects = {}
    #print(inputQuery)
    for term in inputQuery:
        if term in index:
            for data in list_of_data:
                for tupl in index[term]:
                    if data.ID in tupl:
                        dataObjects[tupl[data.ID] * inverseDocumentFreqIndex[term]] = data
        else:
            for i in range(len(term),0,-1):
                if term[0:i] in index:
                    #print("Term", term[0:i])
                    tempTerm = term[0:i]
                    for data in list_of_data:
                        for tupl in index[tempTerm]:
                            if data.ID in tupl:
                                dataObjects[tupl[data.ID] * inverseDocumentFreqIndex[tempTerm]] = data
                    break
                           
    #print("DATAOBJECTS", dataObjects)
    return dataObjects

def print_output(dataObjects):
    global actualInputKeyWords
    global expectedInputKeyWords
    keyWordsForOutput = []
    for keyWord in actualInputKeyWords:
        if keyWord in expectedInputKeyWords:
            keyWordsForOutput.append(keyWord)
    print("------------------------------")
    print("Vstupne keywords: ", keywords)
    print("------------------------------")
    if dataObjects:
        for data in dataObjects.values():
            if keyWordsForOutput:
                print("ID :", data.ID)
                if 'titul' in keyWordsForOutput:
                    print("Titul :", data.title)
                    print("Subtitul :",data.subtitle)
                if 'autor' in keyWordsForOutput:
                    print("Autor :", data.autor)
                if 'abstrakt' in keyWordsForOutput:
                    print("Abstrakt :", data.abstract)
            else:
                print(data)
            print("------------------------------")
    else:
        print("Nebola najdena zhoda -> vypisujem vsetko")
        for data in list_of_data:
            print(data)
            print("------------------------------")

    print("Najpouzivanejsie termy")
    print("------------------------------")
    #printIndex(20)

def runProgram():
    load_stopwords('cz')
    load_documents()
    index_documents()
    list_of_objects = read_input()
    #analyze()
    print_output(list_of_objects)

# run program
runProgram()