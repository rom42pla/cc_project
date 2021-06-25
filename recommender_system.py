import time

from pyspark.ml.evaluation import RegressionEvaluator
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

    als = ALS(maxIter=iterations, regParam=0.01, userCol="user_id", itemCol="product_id", ratingCol="rating")
    model = als.fit(df)

    total_time = time.time() - starting__time
    if logs:
        print(f"Training time: {int(total_time)}s")

    return model

def evaluate_recommender_system(df: DataFrame,
                                model,
                                logs: bool = False):
    starting__time = time.time()
    if logs:
        print(f"Starting evaluation of ALS model")

    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(model.transform(df).na.drop())

    total_time = time.time() - starting__time
    if logs:
        print(f"Evaluation time: {int(total_time)}s")
        print(f"Root-mean-square error: {rmse}")
