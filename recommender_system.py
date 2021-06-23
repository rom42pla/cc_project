import time

from pyspark.sql.functions import udf
from pyspark.ml.recommendation import ALS
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import IntegerType


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
