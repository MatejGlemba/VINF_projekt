from os import path
import regex
import lucene
from lucene import *
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.document import Document, Field, TextField, StoredField
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser
import csv
import cze_stemmer
import sys

def stemming(query):
    return cze_stemmer.cz_stem(query)

# -------- INDEXING --------
lucene.initVM(vmargs=['-Djava.awt.headless=true'])
#print('lucene', lucene.VERSION)

if not path.exists('index') and not path.exists('index2'):
    print("Creating index...")
    store = SimpleFSDirectory(Paths.get("index"))
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(store, config)

    # Create document
    with open('data.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            doc = Document()
            #print(row)
            doc.add(Field("id", row['id'], TextField.TYPE_STORED))
            doc.add(Field("autor", row['autor'], TextField.TYPE_STORED))
            doc.add(Field("originalAutor", row['originalAutor'], TextField.TYPE_STORED))
            doc.add(Field("title", row['title'], TextField.TYPE_STORED))
            doc.add(Field("originalTitle", row['originalTitle'], TextField.TYPE_STORED))
            doc.add(Field("subtitle", row['subtitle'], TextField.TYPE_STORED))
            doc.add(Field("konspekt", row['konspekt'], TextField.TYPE_STORED))
            doc.add(Field("podkonspekt", row['podkonspekt'], TextField.TYPE_STORED))
            doc.add(Field("abstract", row['abstract'], TextField.TYPE_STORED))
            doc.add(Field("text", row['text'], TextField.TYPE_STORED))
            doc.add(StoredField("originalText", row['originalText']))
            writer.addDocument(doc)
    writer.commit()
    writer.close()
    
    
    # Create document
    store2 = SimpleFSDirectory(Paths.get("index2"))
    analyzer2 = StandardAnalyzer()
    config2 = IndexWriterConfig(analyzer2)
    config2.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer2 = IndexWriter(store2, config2)
    
    with open('dataCsv.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            doc = Document()
            #print(row)
            doc.add(Field("transakciaID", row['transakciaID'], TextField.TYPE_STORED))
            doc.add(Field("timestamp", row['timestamp'], TextField.TYPE_STORED))
            doc.add(Field("operacia", row['operacia'], TextField.TYPE_STORED))
            doc.add(Field("catId", row['catId'], TextField.TYPE_STORED))
            doc.add(Field("cena", row['cena'], TextField.TYPE_STORED))
            doc.add(Field("userHash", row['userHash'], TextField.TYPE_STORED))
            doc.add(Field("psc", row['psc'], TextField.TYPE_STORED))
            doc.add(Field("vek", row['vek'], TextField.TYPE_STORED))
            doc.add(Field("casVypozicania", row['casVypozicania'], TextField.TYPE_STORED))
            doc.add(Field("dlzkaTransakcie", row['dlzkaTransakcie'], TextField.TYPE_STORED))
            doc.add(Field("pocetUpomienok", row['pocetUpomienok'], TextField.TYPE_STORED))
            writer2.addDocument(doc)
    writer2.commit()
    writer2.close()

# -------- SEARCHING --------
print("Searching...")
directory = SimpleFSDirectory(Paths.get("index"))
searcher = IndexSearcher(DirectoryReader.open(directory))

directory = SimpleFSDirectory(Paths.get("index2"))
searcher2 = IndexSearcher(DirectoryReader.open(directory))

analyzer = StandardAnalyzer()


inputKeys = sys.argv[1].strip()
top = sys.argv[2]
#print(inputKeys)
if regex.search(",", inputKeys):
    inputKeys = regex.split(",", inputKeys)
    tempString = ""
    for inputKey in inputKeys:
        tempString += stemming(inputKey.strip()) + " "
    inputKeys = tempString
else:
    inputKeys = regex.finditer("(?P<prefix>[^:]*:)?(?P<key>[^\^ [AND|OR]+]*)(?P<postfix>\^[0-9])?(?P<op>[AND|OR| ]+)?",inputKeys)
    if inputKeys:
        #print("GROUPS")
        tempString = ""
        for inputKey in inputKeys:
            inputKeyDict = inputKey.groupdict()
            if inputKeyDict['prefix']:
                print("PREFIX", inputKeyDict['prefix'])
                tempString += inputKeyDict['prefix']
            if inputKeyDict['key']:
                print("KEY", inputKeyDict['key'])
                tempString += stemming(inputKeyDict['key'].lower())
            if inputKeyDict['postfix']:
                print("POSTFIX", inputKeyDict['postfix'])
                tempString += inputKeyDict['postfix']
            if inputKeyDict['op']:
                print("OP", inputKeyDict['op'])
                tempString += inputKeyDict['op']  
        inputKeys = tempString
    else:
        pass
        #print("NIC")    
print(inputKeys)

query = QueryParser("text", analyzer).parse(inputKeys)
scoreDocs = searcher.search(query, int(top)).scoreDocs

print("%s total matching documents." % len(scoreDocs))
option = ''
if len(scoreDocs) > 0:
    print("--------------------------------")
    print("1 - operacie podla autora")
    print("2 - konspekty podla autora")
    print("3 - okres podla autora")
    print("4 - vekova skupina podla autora")
    print("0 - koniec")
    print("--------------------------------")
    option = input()

    if option == 1:
        if not path.exists('indexOperacie'):
            print("Creating indexOperacie...")
            storeOP = SimpleFSDirectory(Paths.get("indexOperacie"))
            analyzerOP = StandardAnalyzer()
            configOP = IndexWriterConfig(analyzerOP)
            configOP.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            writerOP = IndexWriter(storeOP, configOP)

            # Create document
            with open('operacie.csv', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    doc = Document()
                    #print(row)
                    doc.add(Field("originalAutor", row['originalAutor'], TextField.TYPE_STORED))
                    doc.add(Field("operacia", row['operacia'], TextField.TYPE_STORED))
                    doc.add(Field("count", row['count'], TextField.TYPE_STORED))
                    writerOP.addDocument(doc)
            writerOP.commit()
            writerOP.close()
        directoryOP = SimpleFSDirectory(Paths.get("indexOperacie"))
        searcherOP = IndexSearcher(DirectoryReader.open(directoryOP))
        analyzerOP = StandardAnalyzer()
        
        for scoreDoc in scoreDocs:
            print("---------------------------------------")
            doc = searcher.doc(scoreDoc.doc)
            print('Score: ', scoreDoc, 'Data: ', doc.get('originalText'))
            print("----------------STATS------------------")
            queryOP = QueryParser("originalAutor", analyzerOP).parse(doc.get('originalAutor'))
            scoreDocsOP = searcherOP.search(queryOP, 15).scoreDocs
            print("%s total matching OP documents." % len(scoreDocsOP))
            for scoreDocOP in scoreDocsOP: 
                print(scoreDocOP)
    elif option == 2:
        if not path.exists('indexKonspekt'):
            print("Creating indexKonspekt...")
            storeKON = SimpleFSDirectory(Paths.get("indexKonspekt"))
            analyzerKON = StandardAnalyzer()
            configKON = IndexWriterConfig(analyzerKON)
            configKON.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            writerKON = IndexWriter(storeKON, configKON)

            # Create document
            with open('konspekt.csv', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    doc = Document()
                    #print(row)
                    doc.add(Field("originalAutor", row['originalAutor'], TextField.TYPE_STORED))
                    doc.add(Field("konspekt", row['konspekt'], TextField.TYPE_STORED))
                    doc.add(Field("count", row['count'], TextField.TYPE_STORED))
                    writerKON.addDocument(doc)
            writerKON.commit()
            writerKON.close()
        directoryKON = SimpleFSDirectory(Paths.get("indexKonspekt"))
        searcherKON = IndexSearcher(DirectoryReader.open(directoryKON))
        analyzerKON = StandardAnalyzer()
        
        for scoreDoc in scoreDocs:
            print("---------------------------------------")
            doc = searcher.doc(scoreDoc.doc)
            print('Score: ', scoreDoc, 'Data: ', doc.get('originalText'))
            print("----------------STATS------------------")
            queryKON = QueryParser("originalAutor", analyzerKON).parse(doc.get('originalAutor'))
            scoreDocsKON = searcherKON.search(queryKON, 15).scoreDocs
            print("%s total matching OP documents." % len(scoreDocsKON))
            for scoreDocKON in scoreDocsKON: 
                print(scoreDocKON)
    elif option == 3:
        if not path.exists('indexOkres'):
            print("Creating indexOkres...")
            storeOKR = SimpleFSDirectory(Paths.get("indexOkres"))
            analyzerOKR = StandardAnalyzer()
            configOKR = IndexWriterConfig(analyzerOKR)
            configOKR.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            writerOKR = IndexWriter(storeOKR, configOKR)

            # Create document
            with open('psc.csv', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    doc = Document()
                    #print(row)
                    doc.add(Field("originalAutor", row['originalAutor'], TextField.TYPE_STORED))
                    doc.add(Field("psc", row['psc'], TextField.TYPE_STORED))
                    doc.add(Field("count", row['count'], TextField.TYPE_STORED))
                    writerOKR.addDocument(doc)
            writerOKR.commit()
            writerOKR.close()
        directoryOKR = SimpleFSDirectory(Paths.get("indexOkres"))
        searcherOKR = IndexSearcher(DirectoryReader.open(directoryOKR))
        analyzerOKR = StandardAnalyzer()
           
        for scoreDoc in scoreDocs:
            print("---------------------------------------")
            doc = searcher.doc(scoreDoc.doc)
            print('Score: ', scoreDoc, 'Data: ', doc.get('originalText'))
            print("----------------STATS------------------")
            queryOKR = QueryParser("originalAutor", analyzerOKR).parse(doc.get('originalAutor'))
            scoreDocsOKR = searcherOKR.search(queryOKR, 15).scoreDocs
            print("%s total matching OP documents." % len(scoreDocsOKR))
            for scoreDocOKR in scoreDocsOKR: 
                print(scoreDocOKR)
    elif option == 4:
        if not path.exists('indexVek'):
            print("Creating indexVek...")
            storeVEK = SimpleFSDirectory(Paths.get("indexVek"))
            analyzerVEK = StandardAnalyzer()
            configVEK = IndexWriterConfig(analyzerVEK)
            configVEK.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            writerVEK = IndexWriter(storeVEK, configVEK)

            # Create document
            with open('vekSkupina.csv', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    doc = Document()
                    #print(row)
                    doc.add(Field("originalAutor", row['originalAutor'], TextField.TYPE_STORED))
                    doc.add(Field("vekSkupina", row['vekSkupina'], TextField.TYPE_STORED))
                    doc.add(Field("count", row['count'], TextField.TYPE_STORED))
                    writerVEK.addDocument(doc)
            writerVEK.commit()
            writerVEK.close()
        directoryVEK = SimpleFSDirectory(Paths.get("indexVek"))
        searcherVEK = IndexSearcher(DirectoryReader.open(directoryVEK))
        analyzerVEK = StandardAnalyzer()
           
        for scoreDoc in scoreDocs:
            print("---------------------------------------")
            doc = searcher.doc(scoreDoc.doc)
            print('Score: ', scoreDoc, 'Data: ', doc.get('originalText'))
            print("----------------STATS------------------")
            queryVEK = QueryParser("originalAutor", analyzerVEK).parse(doc.get('originalAutor'))
            scoreDocsVEK = searcherVEK.search(queryVEK, 15).scoreDocs
            print("%s total matching OP documents." % len(scoreDocsVEK))
            for scoreDocVEK in scoreDocsVEK: 
                print(scoreDocVEK)
    else:
        for scoreDoc in scoreDocs:
            print("---------------------------------------")
            doc = searcher.doc(scoreDoc.doc)
            print('Score: ', scoreDoc, 'Data: ', doc.get('originalText'))