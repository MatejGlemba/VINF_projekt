#!/usr/bin/env python
# encoding=utf8  

import sys
from pyspark import rdd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import col, asc, desc
import unicodedata
import regex
import cze_stemmer

schema_for_xml = StructType([
    StructField("id", StringType(), True),
    StructField("autor", StringType(), True),
    StructField("originalAutor", StringType(), True),
    StructField("title", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("subtitle", StringType(), True),
    StructField("abstract", StringType(), True),
    StructField("konspekt", StringType(), True),
    StructField("podkonspekt", StringType(), True),
    StructField("text", StringType(), True),
    StructField("originalText", StringType(), True)
])

schema_for_csv = StructType([
    StructField("transakciaID", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("operacia", StringType(), True),
    StructField("catId", StringType(), True),
    StructField("cena", StringType(), True),
    StructField("userHash", StringType(), True),
    StructField("psc", StringType(), True),
    StructField("vek", StringType(), True),
    StructField("vekSkupina", StringType(), True),
    StructField("casVypozicania", StringType(), True),
    StructField("dlzkaTransakcie", StringType(), True),
    StructField("pocetUpomienok", StringType(), True)
])

konspektValues = {
    1 : "antropologia, etnografia",
    2 : "biologicke vedy",
    3 : "divadlo, film, tanec",
    4 : "ekonomicke vedy, obchod",
    5 : "filozofia a nabozenstvo",
    6 : "fyzika a pribuzne vedy",
    7 : "geografia a geologia",
    8 : "historia",
    9 : "hudba",
    10 : "chemia",
    11 : "jazyk, lingvistika",
    12 : "knihovnictvo, informatika",
    13 : "matematika",
    14 : "lekarstvo",
    15 : "politicke vedy",
    16 : "pravo",
    17 : "psychologia",
    18 : "sociologia",
    19 : "technika, technologie",
    20 : "telesna vychova a sport",
    21 : "umenie, architektura",
    22 : "vychova a vzdelavanie",
    23 : "vypoctova technika",
    24 : "polnohospodarstvo",
    25 : "beletria",
    26 : "literatura pre deti a mladez"
}

operacie = {
    "V" : "vypozicka",
    "N" : "navrat",
    "P" : "prolongacia",
    "U1" : "upomienka-1",
    "U2" : "upomienka-2",
    "U3" : "upomienka-3",
    "U4" : "upomienka-4",
    "U5" : "upomienka-5",
    "U6" : "upomienka-6",
    "U7" : "upomienka-7",
    "U8" : "upomienka-8",
    "U9" : "upomienka-9",
    "Z" : "ziadanka",
    "H" : "nevyziadana ziadanka",
    "R" : "rezervacia",
    "L" : "platba",
    "E" : "presun",
    "S" : "statistika",
    "W" : "volny vyber",
    "M" : "mvs",
    "K" : "kopia",
    "G" : "e-vypozicka"
}

pscValues = {
    37367 : "borek",
    37312 : "borovany",
    37382 : "borsov nad vltavou",
    37362 : "chotycany",
    37304 : "chrastany",
    37316 : "dobra voda u ceskych budejovic",
    37365 : "dolni bukovsko",
    37008 : "dubravice",
    37006 : "dubicne",
    37364 : "dynin",
    37348 : "divcice",
    37351 : "driten",
    37341 : "hluboka nad vltavou",
    37335 : "horni stropnice",
    37372 : "hvozdec",
    37384 : "jankov",
    37332 : "jilovice",
    37314 : "komarice",
    37311 : "ledenice",
    37322 : "locenice",
    37363 : "mazelov",
    37901 : "mladosovice",
    37349 : "mydlovary",
    37333 : "nove hrady",
    37331 : "olesnice",
    37350 : "olesnik",
    37346 : "pistin",
    37007 : "plav",
    37371 : "rudolfov",
    37321 : "slavve",
    37323 : "svaty jan nad malsi",
    37305 : "temelin",
    37401 : "trhove sviny",
    37501 : "tyn nad vltavou",
    37344 : "zliv",
    37010 : "usilne",
    39165 : "cenkov u bechyne",
    37001 : "ceske budejovice",
    37373 : "stepanovice",
    37366 : "zimutice"
}

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
 
def processKonspekt(konspekt):
    if konspekt:
        return konspektValues[int(konspekt)]
    return ''

def processOperation(op):
    if op:
        return operacie[op]
    return ''

def processPsc(psc):
    if psc:
        psc = regex.sub("\s", "", psc)
        try:
            psc = int(psc)
        except ValueError:
            return ''
        if psc in pscValues:
            return pscValues[int(psc)]
    return ''

def processCatId(catId):
    if catId:
        return regex.split("\*", catId)[1]
    return catId

def processCena(cena):
    if cena:
        cena = regex.sub("Kč ", "", cena)
        cena = regex.sub("\.", ",", cena)
    return str(cena)

def processDate(date):
    if date:
        return date[0:4] + "-" + date[4:6] + "-" + date[6:8]# + "-" + date[8:10] + ":" + date[10:12]
    return date

def processVek(vek):
    if vek and vek.isdigit():
        vek = int(vek)
        if vek < 20:
            return 1
        elif vek < 30:
            return 2
        elif vek < 40:
            return 3
        elif vek < 50:
            return 4
        elif vek < 60:
            return 5
        elif vek < 70:
            return 6
        elif vek < 100:
            return 7
        else: 
            return 0
    return 0

def processCsv(row):
    resultsArray = []
    results = {}
    results['transakciaID'] = row['ArlID']
    results['timestamp'] = processDate(str(row['tcreate'])) 
    results['operacia'] = processOperation(str(row['op']))
    results['catId'] = processCatId(str(row['catId']))
    results['cena'] = processCena(str(row['Tcat_T020c']))
    results['userHash'] = row['userHash']
    results['psc'] = processPsc(row['psc'])
    results['vek'] = row['vek']
    results['vekSkupina'] = processVek(row['vek'])
    results['casVypozicania'] = row['casVypujceni']
    results['dlzkaTransakcie'] = row['DelkaTransakce']
    results['pocetUpomienok'] = row['pocetUpominek']
    resultsArray.append(results)
    return resultsArray

def processAutor(autor):
    if autor:
        splitArr = regex.split(",", autor)
        splitArr = list(reversed(splitArr))
        autor = ''
        for s in splitArr:
            if s:
                autor += s + ' '
        #return str(splitArr)
    return autor.strip()
            
def process(controlfield, datafield):
    """
    Read the xml string from rdd, parse and extract the elements,
    then return a list of list.
    """
    resultsArray = []
    controlfield_text = str(controlfield)
    controlfield_text = regex.search("Row\(_VALUE='(?P<id>[^']*)', _tag=1\)", controlfield_text)
    if controlfield_text:
        controlfield_text = controlfield_text.group('id')
    rdd_text = str(datafield)
    rdd_text = regex.sub("u'", "'", rdd_text)
    data = regex.finditer("(Row\(_ind1='1', _ind2=' ', _tag='100', subfield=\[(Row\(_VALUE='(?P<autor>[^']*)', _code='a'\))?)(.+?Row\(_ind1='1', _ind2='0', _tag='245', subfield=\[Row\(_VALUE='(?P<title>[^']*)', _code='a'\)(, Row\(_VALUE='(?P<subtitle>[^']*)', _code='b'\))?)?(.+?_tag='520', subfield=\[(Row\(_VALUE='(?P<abstract>[^']*)', _code='a'\))?)?(.+?_tag='C72', subfield=\[(Row\(_VALUE='.+?', _code='a'\))?(, Row\(_VALUE='(?P<konspekt>\d+)', _code='9'\))?(, Row\(_VALUE='(?P<podkonspekt>[^']*)', _code='x'\))?\]\))?", rdd_text)
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
        konspekt = data_dict['konspekt']
        if konspekt is None:
            konspekt = ""
        podkonspekt = data_dict['podkonspekt']
        if podkonspekt is None:
            podkonspekt = ""     
        results['originalText'] = "Autor: " + autor + ", Title: " + title + ", Subtitle: " + subtitle + ", Abstract: " + abstract 
        results['id'] = controlfield_text
        results['autor'] = processData(processAutor(autor))
        results['originalAutor'] = processAutor(autor)
        results['title'] = processData(title)
        results['originalTitle'] = title
        results['subtitle'] = processData(subtitle)
        results['abstract'] = processData(abstract)
        results['konspekt'] = processKonspekt(konspekt)
        results['podkonspekt'] = podkonspekt
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
    inputFileXml = sys.argv[1]
    if not inputFileXml:
        inputFileXml = "data-short.xml"
    inputFileCsv = sys.argv[3]
    if not inputFileCsv:
        inputFileCsv = "transactions-short.csv"
    
    spark = SparkSession.builder.getOrCreate()
    #-------------- spracovanie XML -------------#
    df = spark.read.format("com.databricks.spark.xml")\
        .options(rowTag='record', rootTag='collection', encode='utf-8')\
        .load(inputFileXml)
    #print(df.take(2))
    df.printSchema()
    selected = df.select("datafield", "controlfield")\
        .dropna(how='any')\
        .rdd\
        .flatMap(lambda row: process(row['controlfield'], row['datafield']))
    #print(selected.collect())
    df2 = spark.createDataFrame(data=selected, schema = schema_for_xml)
    df2.printSchema()
    df2.show(5, False)
    #-------------- spracovanie CSV -------------# 
    dfcsv = spark.read.format('com.databricks.spark.csv').option("header", True)\
        .csv(inputFileCsv)
    dfcsv.printSchema()
    selectedCsv = dfcsv.select("ArlID","tcreate","op","catId","Tcat_T020c","userHash","psc", "vek","casVypujceni","DelkaTransakce","pocetUpominek")\
        .dropna(how='any')\
        .rdd\
        .flatMap(lambda row: processCsv(row))
    #print(selectedCsv.collect())        
    dfcsv2 = spark.createDataFrame(data=selectedCsv, schema=schema_for_csv)
    dfcsv2.printSchema()
    #-------------- vytvaranie statistickych tabuliek -------------#
    tempDf2 = df2
    tempCsv2 = dfcsv2
    joindf = tempDf2.join(tempCsv2,tempDf2.id ==  tempCsv2.catId,"inner")\
        .show(truncate=False)
    
    tempDf2 = df2
    tempCsv2 = dfcsv2
    autorOperaciaDF = tempDf2.join(tempCsv2,tempDf2.id ==  tempCsv2.catId,"inner")\
        .filter(col("originalAutor").isNotNull())\
        .filter(col("operacia").isNotNull())\
        .filter(col("operacia") != '')\
        .groupBy("originalAutor", "operacia") \
        .count().alias("count")\
        .orderBy(col("count").desc(),col("originalAutor").desc(),col("operacia").desc())
        #.show(truncate=False)
    
    #tempDf2 = df2
    #tempCsv2 = dfcsv2
        
    #autorKonspektDF = tempDf2.join(tempCsv2,tempDf2.id ==  tempCsv2.catId,"inner")\
    #    .filter(col("originalAutor").isNotNull())\
    #    .filter(col("konspekt").isNotNull())\
    #    .filter(col("konspekt") != '')\
    #    .groupBy("originalAutor", "konspekt")\
    #    .count().alias("count")\
    #    .orderBy(col("count").desc(),col("originalAutor").desc(),col("konspekt").desc())\
    #    .show(truncate=False)
        
    tempDf2 = df2
    tempCsv2 = dfcsv2
        
    autorPscDF = tempDf2.join(tempCsv2,tempDf2.id ==  tempCsv2.catId,"inner")\
        .filter(col("originalAutor").isNotNull())\
        .filter(col("psc").isNotNull())\
        .filter(col("psc") != '')\
        .groupBy("originalAutor", "psc") \
        .count().alias("count")\
        .orderBy(col("count").desc(),col("originalAutor").desc(),col("psc").desc())
        #.show(truncate=False)
        
    tempDf2 = df2
    tempCsv2 = dfcsv2
    
    autorVekSkupinaDF = tempDf2.join(tempCsv2,tempDf2.id ==  tempCsv2.catId,"inner")\
        .filter(col("originalAutor").isNotNull())\
        .filter(col("vekSkupina").isNotNull())\
        .filter(col("vekSkupina") != 0)\
        .groupBy("originalAutor", "vekSkupina") \
        .count().alias("count")\
        .orderBy(col("count").desc(),col("originalAutor").desc(),col("vekSkupina").desc())
        #.show(truncate=False)
        
    tempDf2 = df2
    tempCsv2 = dfcsv2
    
    #joindf5 = tempDf2.join(tempCsv2,tempDf2.id ==  tempCsv2.catId,"inner")\
    #    .groupBy("originalAutor", "transakciaID", "operacia") \
    #    .count().alias("count")\
    #    .orderBy(col("count").desc(),col("transakciaID").desc())\
    #    .show(truncate=False)
    
    
    #------------ zapis dat do csv -------------------------------#
    outputFileXml = sys.argv[2]
    if not outputFileXml:
        outputFileXml = "dataXml"
        
    outputFileCsv = sys.argv[4]
    if not outputFileCsv:
        outputFileCsv = "dataCsv"
    
    df2.write.option("delimiter", ",")\
        .option("header", "true")\
        .format('com.databricks.spark.csv')\
        .save(outputFileXml)

    dfcsv2.write.option("delimiter", ",")\
        .option("header", "true")\
        .format('com.databricks.spark.csv')\
        .save(outputFileCsv)
    
    #autorKonspektDF.write.option("delimiter", ",")\
    #    .option("header", "true")\
    #    .format('com.databricks.spark.csv')\
    #    .save("konspekt")
    autorOperaciaDF.write.option("delimiter", ",")\
        .option("header", "true")\
        .format('com.databricks.spark.csv')\
        .save("operacie")
    autorPscDF.write.option("delimiter", ",")\
        .option("header", "true")\
        .format('com.databricks.spark.csv')\
        .save("psc")
    autorVekSkupinaDF.write.option("delimiter", ",")\
        .option("header", "true")\
        .format('com.databricks.spark.csv')\
        .save("vekSkupina")
    
# run program
if __name__ == '__main__':
    runProgram()