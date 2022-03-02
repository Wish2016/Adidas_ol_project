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
filter_df = filter_df.withColumn("work_titles", F.explode_outer("other_titles"))

# All Harry Potter Books
spark.sql('''select distinct key, title from filter_df where lower(title) like '%harry potter%' 
or lower(work_titles) like '%harry potter%' or lower(other_titles) like '%harry potter%' ''').show(20, False)

# Get the book with most pages.
spark.sql('''select key, title, number_of_pages from filter_df order by 3 desc limit 1''').show(20, False)

# Find the Top 5 authors with most written books (assuming author in first position in the array, "key" field and each
# row is a different book)
spark.sql('''select author, count(distinct key) as books_count 
from filter_df group by 1 order by 2 desc limit 5''').show(20, False)

# Find the Top 5 genres with most books
spark.sql('''select genre, count(distinct key) as books_count from filter_df where genre is not null 
group by 1 order by 2 desc limit 5 ''').show(20, False)

# Get the avg. number of pages.
spark.sql('''select round(avg(number_of_pages)) as avg_pages from
 (select distinct key, number_of_pages from filter_df) dt ''').show(20, False)

# Per publish year, get the number of authors that published at least one book
spark.sql('''select publish_date, count(author) from (select publish_date, author, count(distinct key) from filter_df
group by 1,2 having count(distinct key) >=1) dt group by 1 order by 1 ''').show(20, False)




