#!/usr/bin/env python
# encoding=utf8  

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
import unicodedata
import regex
import cze_stemmer

extract_record_info_schema = StructType([
    StructField("autor", StringType(), True),
    StructField("title", StringType(), True),
    StructField("subtitle", StringType(), True),
    StructField("abstract", StringType(), True),
    StructField("text", StringType(), True),
    StructField("originalText", StringType(), True)
])

def load_stopwords():
    global stopWords
    with open('stopwords.txt', encoding='utf-8', mode='r') as f:
        stopWords = [line.strip() for line in f ]
    print(stopWords)
  
def removeDiacritics(text):
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
        if term not in stopWords or len(term) > 3:
            new_list_of_terms.append(term)
    list_of_terms = None
    return new_list_of_terms
    
def process(rdd):
    """
    Read the xml string from rdd, parse and extract the elements,
    then return a list of list.
    """
    resultsArray = []
    rdd_text = str(rdd)
    rdd_text = regex.sub("u'", "'", rdd_text)
    data = regex.finditer("Row\(_ind1='1', _ind2='0', _tag='245', subfield=\[Row\(_VALUE='(?P<title>[^']*)', _code='a'\)(, Row\(_VALUE='(?P<subtitle>[^']*)', _code='b'\))?(, Row\(_VALUE='(?P<autor>[^']*)', _code='c'\))?](.+?_tag='520', subfield=\[(Row\(_VALUE='(?P<abstract>[^']*)', _code='a'\))?)?", rdd_text)
    for d in data:
        results = {}
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
        results['originalText'] = "Autor: " + autor + ", Title: " + title + ", Subtitle: " + subtitle + ", Abstract: " + abstract 
        results['autor'] = processData(autor)
        results['title'] = processData(title)
        results['subtitle'] = processData(subtitle)
        results['abstract'] = processData(abstract)
        results['text'] = results['autor'] + results['title'] + results['subtitle'] + results['abstract']
        resultsArray.append(results)
    return resultsArray

def stemming(query):
    stemmedList = ""
    for word in query:
        stemmedList += cze_stemmer.cz_stem(word) + " "
    return stemmedList
    
def processData(data):
    merged_data = data.lower()
    merged_data = removeStopWords(merged_data) 
    merged_data = stemming(merged_data)
    return merged_data

def runProgram():
    load_stopwords()
    inputFile = sys.argv[1]
    if not inputFile:
        inputFile = "data-short.xml"
        
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("com.databricks.spark.xml")\
        .options(rowTag='record', rootTag='collection', encode='utf-8')\
        .load(inputFile)
    #print(df.take(2))
    df.printSchema()
    selected = df.select("datafield")\
        .dropna(how='any')\
        .rdd\
        .flatMap(lambda row: process(row['datafield']))
    #print(selected.collect())
    df2 = spark.createDataFrame(data=selected, schema = extract_record_info_schema)
    df2.printSchema()
    #df2.show(3, False)
    
    outputFile = sys.argv[2]
    if not outputFile:
        outputFile = "data"
    
    df2.write.option("delimiter", ",")\
        .option("header", "true")\
        .format('com.databricks.spark.csv')\
        .save(outputFile)
    
# run program
runProgram()