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

if not path.exists('index'):
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
            doc.add(Field("autor", row['autor'], TextField.TYPE_STORED))
            doc.add(Field("title", row['title'], TextField.TYPE_STORED))
            doc.add(Field("subtitle", row['subtitle'], TextField.TYPE_STORED))
            doc.add(Field("abstract", row['abstract'], TextField.TYPE_STORED))
            doc.add(Field("text", row['text'], TextField.TYPE_STORED))
            doc.add(StoredField("originalText", row['originalText']))
            writer.addDocument(doc)
    writer.commit()
    writer.close()

# -------- SEARCHING --------
print("Searching...")
directory = SimpleFSDirectory(Paths.get("index"))
searcher = IndexSearcher(DirectoryReader.open(directory))
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
for scoreDoc in scoreDocs:
    print("---------------------------------------")
    doc = searcher.doc(scoreDoc.doc)
    print('Score: ', scoreDoc, 'Data: ', doc.get('originalText'))