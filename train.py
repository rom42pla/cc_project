import time
from os.path import join

from pyspark import SparkContext, SparkConf
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession

import recommender_system
from datasets import preprocess_dataframe

starting_time = time.time()

# creates session and context
conf = SparkConf().set("spark.ui.port", "4050")
spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

# reads the dataframe
df_path = join(".", "preprocessed_dataset.tsv")
df = spark.read.csv(df_path, header=True, inferSchema=True, sep="\t")
# df = preprocess_dataframe(df=df)
# df.cache()
df.printSchema()
print(f"|ratings| = {df.count()}, \t"
      f"|users| = {df.select('user_id').distinct().count()}, \t"
      f"|items| = {df.select('product_id').distinct().count()}")
df.show()

# training the model
df_train, df_test = df.randomSplit([0.8, 0.2])
model = recommender_system.train_recommender_system(df=df_train, iterations=10, logs=True)
# model.save("model")

# evaluating the model
rmse = recommender_system.evaluate_recommender_system(df=df_test, model=model, logs=True)

total_time = time.time() - starting_time
print(f"Total time: {int(total_time)}s")
