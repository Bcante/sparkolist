#imports
import sys
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql.functions import *

sc = SparkContext()
sqlContext = SQLContext(sc)

customers = sqlContext.read.csv("../csv/olist_customers_dataset.csv", header="true", inferSchema="true")
geo = sqlContext.read.csv("../csv/olist_geolocation_dataset.csv", header="true", inferSchema="true")
items = sqlContext.read.csv("../csv/olist_order_items_dataset.csv", header="true", inferSchema="true")
payments = sqlContext.read.csv("../csv/olist_order_payments_dataset.csv", header="true", inferSchema="true")
reviews = sqlContext.read.csv("../csv/olist_order_reviews_dataset.csv", header="true", inferSchema="true")
orders = sqlContext.read.csv("../csv/olist_orders_dataset.csv", header="true", inferSchema="true")
products = sqlContext.read.csv("../csv/olist_products_dataset.csv", header="true", inferSchema="true")
sellers = sqlContext.read.csv("../csv/olist_sellers_dataset.csv", header="true", inferSchema="true") 
translation = sqlContext.read.csv("../csv/product_category_name_translation.csv", header="true", inferSchema="true") 
