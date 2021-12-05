#!/usr/bin/env python
# encoding=utf8  

from re import UNICODE
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from unidecode import unidecode
import unicodedata
#import cze_stemmer
import regex

extract_record_info_schema = StructType([
    StructField("autor", StringType(), True),
    StructField("title", StringType(), True),
    StructField("subtitle", StringType(), True),
    StructField("abstract", StringType(), True)
])

def load_stopwords():
    global stopWords
    with open('stopwords_cze.txt', encoding='utf-8', mode='r') as f:
        stopWords = [line.strip() for line in f ]
    print(stopWords)
            
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
    
def removeDiacritics(text):
	try:
		text = UNICODE(text, 'utf-8')
	except NameError:
		pass
	text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
	return str(text)

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
        results['autor'] = processData(autor)
        results['title'] = processData(title)
        results['subtitle'] = processData(subtitle)
        results['abstract'] = processData(abstract)
        resultsArray.append(results)
    return resultsArray

def processData(data):
    merged_data = data.lower()
    merged_data = removeStopWords(merged_data) 
    #merged_data = stemming(merged_data, 'cz')
    return merged_data

def runProgram():
    load_stopwords()
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("com.databricks.spark.xml").options(rowTag='record', rootTag='collection', encode='utf-8').load('stud-Cat_short.xml')
    #print(df.take(2))
    df.printSchema()
    selected = df.select("datafield")\
        .dropna(how='any')\
        .rdd\
        .flatMap(lambda row: process(row['datafield']))
    print(selected.collect())
    df2 = spark.createDataFrame(data=selected, schema = extract_record_info_schema)
    df2.printSchema()
    df2.show(3, False)
    df2.write.option("delimiter", "\t").format('com.databricks.spark.csv').save('output.csv')
    
# run program
runProgram()