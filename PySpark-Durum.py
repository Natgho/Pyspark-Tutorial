# -*- coding:utf-8 -*-
# Sezer BOZKIR - Reidin - 2016 - PySpark Tutorial

# Ornek veriseti çekilebilecek adres: 
# http://grouplens.org/datasets/movielens/

# Pyspark'ı çalıştırma
export PATH=/home/sezer/anaconda2/bin:$PATH
IPYTHON_OPTS='notebook' ./bin/pyspark --packages com.databricks:spark-csv_2.11:1.2.0

# Bir kaynaktan Scala context aracılığıyla veriyi RDD olarak okuma
Veri = sc.textFile("/home/sezer/11072016")

# RDD veriyi ekrana basma (collect asamasi)
Veri.Collect()

''' 
Veri RDD formatındaki elementlere ayrılmakta
Elementler içerisinde gezip, işlem yapabilmekteyiz
'''

# Verinin ilk parçasını yazdırma
Veri.first()

# Verinin elementlerinden bastan itibaren istenilen kısma kadar yazdırma
Veri.take(5)

# Verinin içerisinden rastgele istenilen sayıda istenilen aralıkta ornekler çekme
Veri.takeSample(False,10,2)

# Verinin büyüklüğünü bulma
Veri.Count()

# Postgresql'e bağlanma
'''
Öncelikle bağlanabilmek için Psql'in JDBC'sini indirmek gerekli:
https://jdbc.postgresql.org/download.html.
'''
os.environ['SPARK_CLASSPATH'] = "/path/to/driver/postgresql-9.3-1103.jdbc41.jar"

dataframe_mysql = sqlContext.read.format("jdbc").options( url="jdbc:mysql://:3306/demo",driver = "com.mysql.jdbc.Driver",dbtable = "demotable",user="root", password="XXXXX").load()
dataframe_mysql.show()

# Örnek bir database'den country bilgisini çekme ve ekrana basma
Country = sqlContext.read.format("jdbc").options( url="jdbc:mysql://localhost:3306/world",driver = "com.mysql.jdbc.Driver",dbtable = "Country",user="root", password="XXXXXX").load()
Country.persist()
print Country.columns

#Database'den istenilen satırları çekme
country_name = Country.map(lambda row:(row[0],row[1]))

#Cekilen bir datanın cache'lenmesi
import urllib
f = urllib.urlretrieve ("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", "kddcup.data_10_percent.gz")


data_file = "./kddcup.data_10_percent.gz"
raw_data = sc.textFile(data_file).cache()

'''
RDD'ler ile işlem yaparken Linq mantığıyla veriler işlenmekte. Bu sırada veriler üzerinde işlem yapmak için anonim metod tarzında bir yapı kullanılmakta, bu işlemler "map" veya "flatmap" metodu içerisine yazılan anonim fonksiyonlar ile gerçeklenmekte.

map ile flatmap metodu arasındaki fark, map'in içerisine yazılan lambda metodu her element üzerinde uygulanıp, o elementi bir dizi içerisine koyarken, flatmap adından da anlaşılacağı gibi, segmentleri tek parçaya dönüştürüp, işlemleri bu şekilde uygulamakta.
'''

'''
Confused.txt içeriği:
Confusion is the inability to think as clearly or quickly as you normally do.

You may  have difficulty paying attention to anything , remembering anyone, and making decisions.

Confusion may come to anyone early or late phase of the life, depending on the reason behind it .

Many times, confusion lasts for a very short span and goes away.

Other times, it may be permanent and has no cure. It may have association with delirium or dementia. 

Confusion is more common in people who are in late stages of the life and often occurs when you have stayed in hospital.

Some confused people may have strange or unusual behavior or may act aggressively.

A good way to find out if anyone is confused is to question the person their identity i.e. name, age, and the date.

If they are little not sure or unable to answer correctly, they are confused
'''

# Verinin map ile split edilmesi işlemi
confusedRDD= sc.textFile("confused.txt")
confusedRDD.take(5)
mappedconfusion = confusedRDD.map(lambda line : line.split(" "))
>> mappedfusion.take(5)

# Verinin flatmap ile split edilmesi işlemi
flatMappedConfusion = confusedRDD.flatmap(lambda line : line.split(" "))
>>flatMappedConfusion.take(5)

# RDD içerisinde "confus" geçen elementlerin sayısının bulunması
onlyconfusion = confusedRDD.filter(lambda line : ("confus" in line.lower()))
onlyconfusion.count()

# RDD'nin bir kısmını alma (WithReplacement,fraction,seed(default None))
sampledconfusion = confusedRDD.sample(True,0.5,3)

# Elde olan bir veriyi dağıtık RDD haline getirme(2. parametre verip makinelerin sayısını belirtebilme opsiyonlu) ve birleştirme
abhay_marks = [("physics",85),("maths",75),("chemistry",95)]
ankur_marks = [("physics",65),("maths",45),("chemistry",85)]
abhay = sc.parallelize(abhay_marks)
ankur = sc.parallelize(ankur_marks)
abhay.union(ankur).collect()

# Ortak anahtarlara sahip 2 RDD'nin elementlerini birleştirme
Subject_wise_marks = abhay.join(ankur)
Subject_wise_marks.collect()

# İki RDD arasındaki ortak elementleri bulma
Cricket_team = ["sachin","abhay","michael","rahane","david","ross","raj","rahul","hussy","steven","sourav"]
Toppers = ["rahul","abhay","laxman","bill","steve"]
cricketRDD = sc.parallelize(Cricket_team)
toppersRDD = sc.parallelize(Toppers)
toppercricketers = cricketRDD.intersection(toppersRDD)
toppercricketers.collect()

# Farklı RDD'ler birleştirilirken eş olan verilerin silinmesi
best_story = ["movie1","movie3","movie7","movie5","movie8"]
best_direction = ["movie11","movie1","movie5","movie10","movie7"]
best_screenplay = ["movie10","movie4","movie6","movie7","movie3"]
story_rdd = sc.parallelize(best_story)
direction_rdd = sc.parallelize(best_direction)
screen_rdd = sc.parallelize(best_screenplay)
total_nomination_rdd = story_rdd.union(direction_rdd).union(screen_rdd)
unique_movies_rdd = total_nomination_rdd.distinct()
unique_movies_rdd .collect()

# Okunan veri üzerinde işlem yapılırken kaç parçaya bölüneceğini belirleme
partRDD = sc.textFile("/opt/spark/CHANGES.txt",4)

# RDD'yi işlem için istenilen parçaya kadar bölme
best_story = ["movie1","movie3","movie7","movie5","movie8"]
story_rdd = sc.parallelize(best_story)
story_rdd.getNumPartitions() #6 parca cıktı
story_rdd.coalesce(3).getNumPartitions() #3 parcaya dusurdum ve gosterdim

# Bir veri setinin parse edilmesi
userRDD = sc.textFile("/usr/lib/data_backup/opt/spark_usecases/movie/ml-100k/u.user")
userRDD.count() # 943

def parse_N_calculate_age(data):
             userid,age,gender,occupation,zip = data.split("|")
             return  userid, age_group(int(age)),gender,occupation,zip,int(age)

data_with_age_bucket = userRDD.map(parse_N_calculate_age(data)
RDD_20_30 = data_with_age_bucket.filter( lambda line : '20-30' in line)

# 3. parametreye göre veriyi saydırma
freq = RDD_20_30.map( lambda line : line[3]).countByValue()
dict(age_wise)

# Yazılan bir programı cluster'lar üzerinde ve simple çalıştırma
# Buradaki kodlar bashscript kodlarıdır
# Öncelikle Spark/bin dizini altındaki spark-submit ve pyspark'ın Path'e
#eklenmiş olması gereklidir, ya da elle dizin gösterilmelidir.

spark-submit demo.py #Kodu cluster'lara bölerek çalıştırır
pyspark demo.py #Kodu tek bir makine üzerinde böler

'''

master class vs. belirlemek, memory sınırlamaları için detaylı döküman;
http://spark.apache.org/docs/latest/submitting-applications.html

Spark cluster managers;

    Apache Hadoop YARN: HDFS is the source storage and YARN is the resource manager in this scenario. All read or write operations in this mode are performed on HDFS.
    Apache Mesos: A distributed mode where the resource management is handled by the cluster manager Apache Mesos developed by UC Berkeley.
    Standalone Mode: In this mode the resource management is handled by the spark in-built resource manager.

'''





