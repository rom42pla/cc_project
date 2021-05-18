import time

from pyspark.ml.recommendation import ALS
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType

#todo adjust preprocessing
def preprocess_dataframe(df: DataFrame):
    df = df.select("event_time", "user_id", "event_type", "category_id")
    df = df.filter(df.event_type == "view")#.distinct()
    df = df.withColumn("category_id", df["category_id"].cast(IntegerType()))
    df = df.withColumn("rating", lit(1))
    return df


def train_recommender_system(df: DataFrame,
                             iterations: int = 10,
                             logs: bool = False):
    starting__time = time.time()
    if logs:
        print(f"Starting training of ALS model")

    als = ALS(maxIter=iterations, regParam=0.01, userCol="user_id", itemCol="category_id", ratingCol="rating")
    model = als.fit(df)

    total_time = time.time() - starting__time
    if logs:
        print(f"Training time: {int(total_time)}s")

    return model
