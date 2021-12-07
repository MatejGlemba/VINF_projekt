# VINF_projekt
===== PySpark =====

==== Požiadavky ====

  - docker image iisas/hadoop-spark-pig-hive
  - java 8
  - spark 2.4.3
  - hadoop 2.9.2

==== Flow ====

  - docker run -it --rm --name vinf -e PYSPARK_PYTHON=/usr/bin/python3 -e PYSPARK_DRIVER_PYTHON=python3 -e PYTHONIOENCODING=utf8 -p 8080:8080 iisas/hadoop-spark-pig-hive:2.9.2 bash
    * nastavenie python3 pre pyspark
    * nastavenie python io encoding utf8
  - {vinf_repo}docker cp requirements.txt vinf:/requirements.txt
  - {vinf_repo}docker cp vinf-spark.py vinf:/vinf-spark.py
  - {vinf_repo}docker cp stopwords.txt vinf:/stopwords.txt
  - {vinf_repo}docker cp {data}.xml vinf:/{data}.xml
  - {vinf_repo}docker cp cze_stemmer.py vinf:/cze_stemmer.py
  - {docker container}apt-get update
  - {docker container}apt-get -y install python3-pip
    * nastavenie python3 pip 
  - {docker container}pip3 install -r requirements.txt
  - {docker container}hadoop fs -copyFromLocal {data}.xml
  - {docker container}hadoop fs -copyFromLocal stopwords.txt
  - {docker container}spark-submit --master {master_ip} --executor-memory 512m --packages com.databricks:spark-xml_2.12:0.14.0 --py-files vinf-spark.py,cze_stemmer.py vinf-spark.py {data}.xml {output_file}
  - {docker container}hadoop fs -copyToLocal {output_file}
  - {docker container}mv data/{partition.csv} data.csv
  - {vinf_repo}docker cp vinf:/data.csv .

CSV file ma štruktúru:
  - autor
  - title
  - subtitle
  - abstract
  - text
  - originalText 
 
===== PyLucene =====

==== Požiadavky ====

  - docker image coady/pylucene
  - java 11
  - pylucene 8.9.0

==== Flow ====

  - docker run -it --rm --name lucene coady/pylucene bash
  - {vinf_repo}docker cp cze_stemmer.py lucene:/cze_stemmer.py
  - {vinf_repo}docker cp vinf-pylucene.py lucene:/vinf-pylucene.py
  - {vinf_repo}docker cp data.csv lucene:/data.csv
  - {docker container}pip install regex
  - {docker container}python vinf-pylucene.py "{query_input}" {max_top_results}
    * {query_input} povolené tvary:
      * keywords oddelené medzerou {nad všetkými stĺpcami} = **key1 key2**
      * keywords oddelené medzerou s definovaním stĺpca = **column1:key1 column2:key2**
      * keywords oddelené čiarkou {nad všetkými stĺpcami} = **key1,key2**
      * keywords s použitím operátorov AND/OR {nad všetkými stĺpcami} = **key1 AND key2**
      * keywords s použitím operátorov AND/OR s definovaním stĺpca = **column1:key1 AND column2:key2** 
      * keywords s použitím boost operátora ^{hodnota} {nad všetkými stĺpcami} = **key1^4 key2**
      * keywords s použitím boost operátora ^{hodnota} s definovaním stĺpca = **column1:key1^4 column1:key2**  
      * keywords s použitím boost operátora a AND/OR operátora {nad všetkými stĺpcami} = **key1^4 OR key2**
      * keywords s použitím boost operátora a AND/OR operátora s definovaním stĺpca = **column1:key1^4 OR column2:key2**

