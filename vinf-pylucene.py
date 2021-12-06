# Code from presentation: https://vi2021.ui.sav.sk/lib/exe/fetch.php?media=11_seleng_ir_tools.pdf
# It starts right at the beggining of the presentation

import lucene
from lucene import *
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.document import Document, Field, TextField
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
print('lucene', lucene.VERSION)

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
        doc.add(Field("originalText", row['originalText'], TextField.TYPE_STORED))
        writer.addDocument(doc)
#text = "This is the text to be indexed."


# This may be a costly operation, so you should test the cost
# in your application and do it only when really necessary.
writer.commit()
writer.close()


# -------- SEARCHING --------
# (i guess it's supposed to be separeted file/method. I've merged it just to make it more convinient)
# lucene.initVM(vmargs=['-Djava.awt.headless=true'])
# print('lucene', lucene.VERSION)

directory = SimpleFSDirectory(Paths.get("index"))
searcher = IndexSearcher(DirectoryReader.open(directory))
analyzer = StandardAnalyzer()

# "text" is the value that is searched in indexed text under the field "fieldname"
#scoreDocs = searcher.search(query, 50).scoreDocs

#print("---------------------------------------")
#print("Input: ")
#query = input().strip()
inputKeys = sys.argv[1]
top = sys.argv[2]
inputKeys = inputKeys.lower()
#inputKeys = removeStopWords(inputKeys)
inputKeys = stemming(inputKeys)
print(inputKeys)

inputKeys = "title:rodin^5 OR subtitle:rodin" 
query = QueryParser("text", analyzer).parse(inputKeys)
scoreDocs = searcher.search(query, int(top)).scoreDocs

print("%s total matching documents." % len(scoreDocs))
for scoreDoc in scoreDocs:
    print("---------------------------------------")
    doc = searcher.doc(scoreDoc.doc)
    print('Score: ', scoreDoc, 'Data: ', doc.get('originalText'))