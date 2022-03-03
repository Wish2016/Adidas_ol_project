from pyspark.sql import SparkSession	
import pyspark.sql.functions as F	
spark = SparkSession.Builder().appName("Adidas_data_cleansing").getOrCreate()	

# Loading raw books data	
raw_df = spark.read.format("json").load("C:\\Users\\user\\PycharmProjects\\MySparkSetup\\data\\raw_data\\ol_cdump.json")	

# Creating temp view over raw dataframe	
raw_df.createOrReplaceTempView("initial_data")	
# Filtering raw data on below criteria:	
# title is not null	
# number_of_pages > 20	
# publish_date > 1950	
# authors is not null	
filter_df = spark.sql('''select key,publish_date,number_of_pages,title,work_titles,other_titles,authors,genres 	
from initial_data where title is not null and authors is not null and number_of_pages > 20 and publish_date > 1950''')	


# Flattening required fields	
filter_df = filter_df.withColumn("author", F.explode_outer("authors.key"))	
filter_df = filter_df.withColumn("genre", F.explode_outer("genres"))	
filter_df = filter_df.withColumn("work_titles", F.explode_outer("work_titles"))	
filter_df = filter_df.withColumn("other_titles", F.explode_outer("other_titles"))	
filter_df.createOrReplaceTempView("filter_df")	


# All Harry Potter Books	
spark.sql('''select distinct key, title from filter_df where lower(title) like '%harry potter%' 	
or lower(work_titles) like '%harry potter%' or lower(other_titles) like '%harry potter%' ''').show(20, False)	
+------------------+----------------------------------------+	
|key               |title                                   |	
+------------------+----------------------------------------+	
|/books/OL22858290M|Harry Potter y la piedra filosofal      |	
|/books/OL22856696M|Harry Potter and the philosopher's stone|	
|/books/OL22965924M|Hari Poṭer ṿeha-nasikh ḥatsui ha-dam    |	
|/books/OL22987742M|Lengua franca                           |	
+------------------+----------------------------------------+	


# Get the book with most pages.	
spark.sql('''select key, title, number_of_pages from filter_df order by 3 desc limit 1''').show(20, False)	
+------------------+-----------------------------+---------------+	
|key               |title                        |number_of_pages|	
+------------------+-----------------------------+---------------+	
|/books/OL22855337M|Nihon shokuminchi kenchikuron|48418          |	
+------------------+-----------------------------+---------------+	


# Find the Top 5 authors with most written books (assuming author in first position in the array, "key" field and each	
# row is a different book)	
spark.sql('''select author, count(distinct key) as books_count 	
from filter_df group by 1 order by 2 desc limit 5''').show(20, False)	
+-------------------+-----------+	
|author             |books_count|	
+-------------------+-----------+	
|/authors/OL1224818A|236        |	
|/authors/OL4283462A|116        |	
|/authors/OL785848A |106        |	
|/authors/OL1926829A|80         |	
|/authors/OL883775A |75         |	
+-------------------+-----------+	


# Find the Top 5 genres with most books	
spark.sql('''select genre, count(distinct key) as books_count from filter_df where genre is not null 	
group by 1 order by 2 desc limit 5 ''').show(20, False)	
+--------------------+-----------+	
|genre               |books_count|	
+--------------------+-----------+	
|Fiction.            |3300       |	
|Biography.          |2360       |	
|Juvenile literature.|1538       |	
|Exhibitions.        |836        |	
|Juvenile fiction.   |525        |	
+--------------------+-----------+	


# Get the avg. number of pages.	
spark.sql('''select round(avg(number_of_pages)) as avg_pages from	
 (select distinct key, number_of_pages from filter_df) dt ''').show(20, False)	
 	
+---------+	
|avg_pages|	
+---------+	
|231.0    |	
+---------+	


# Per publish year, get the number of authors that published at least one book	
spark.sql('''select publish_date, count(author) from (select publish_date, author, count(distinct key) from filter_df	
group by 1,2 having count(distinct key) >=1) dt group by 1 order by 1 ''').show(20, False)	
+------------+-------------+	
|publish_date|count(author)|	
+------------+-------------+	
|1951        |648          |	
|1952        |601          |	
|1953        |600          |	
|1954        |644          |	
|1955        |588          |	
|1956        |638          |	
|1957        |705          |	
|1958        |753          |	
|1959        |830          |	
|1960        |920          |	
|1961        |1026         |	
|1962        |1064         |	
|1963        |1099         |	
|1964        |1194         |	
|1965        |1248         |	
|1966        |1203         |	
|1967        |1243         |	
|1968        |1082         |	
|1969        |1209         |	
|1970        |1139         |	
+------------+-------------+	
only showing top 20 rows