# coding: utf8
from re import UNICODE, X
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
import pyspark
import sys
import cze_stemmer
import os
from os import environ
from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType
import regex
environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1 pyspark-shell' 

def load_stopwords():
    global stopWords
    with open('stopwords_cze.txt', mode='r') as f:
        stopWords = [ removeDiacritics(line.strip()) for line in f ]
            
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
    stemmedList = []
    for word in query:
        stemmedList.append(cze_stemmer.cz_stem(word))
    return stemmedList
    
extract_record_info_schema = StructType([
    StructField("autor", StringType(), True),
    StructField("title", StringType(), True),
    StructField("subtitle", StringType(), True),
    StructField("abstract", StringType(), True)
])

def removeDiacritics(text):
	text = text.decode('utf-8')
	return text

def process(rdd):
    """
    Read the xml string from rdd, parse and extract the elements,
    then return a list of list.
    """
    resultsArray = []
    rdd_text = str(rdd).decode('utf-8')
    rdd_text = regex.sub("u'", "'", rdd_text)
    data = regex.finditer("_tag='245', subfield=\[(Row\(_VALUE='(?P<title>.+?)', _code='a'\))(, )?(Row\(_VALUE='(?P<subtitle>.+?)', _code='b'\))?(, )?(Row\(_VALUE='(?P<autor>.+?)', _code='c'\))?(.+?_tag='520', subfield=\[(Row\(_VALUE='(?P<abstract>.+?)', _code='a'\))?)?", rdd_text)
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

def ascii_ignore(x):
    if x:
        return x.decode('utf-8')
    else:
        return None#

ascii_udf = udf(ascii_ignore)

def runProgram():
    load_stopwords()
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format('xml').options(rowTag='record', rootTag='collection', encoding='utf-8').load('stud-Cat_short.xml')
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