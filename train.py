import time
from os.path import join

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

import recommender_system

starting_time = time.time()

# creates session and context
conf = SparkConf().set("spark.ui.port", "4050")
spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

# reads the dataframe
df_path = join(".", "preprocessed_dataset.tsv")
df = spark.read.csv(df_path, header=True, inferSchema=True, sep="\t")
df.printSchema()
print(f"|ratings| = {df.count()}, \t"
      f"|users| = {df.select('user_id').distinct().count()}, \t"
      f"|items| = {df.select('product_id').distinct().count()}")
df.show(4)

# splits the models in train and test
df_train, df_test = df.randomSplit([0.8, 0.2])

# trains the model
model = recommender_system.train_recommender_system(df_train=df_train, df_test=df_test,
                                                    tuning=True, logs=True)

# saves the model
model_path = recommender_system.save_model_zip(model=model, model_name="model", output_folder=".", logs=True)

# evaluates the model
rmse = recommender_system.evaluate_recommender_system(df=df_test, model=model, logs=True)

total_time = time.time() - starting_time
print(f"Total time: {int(total_time)}s")
