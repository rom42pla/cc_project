import os
import shutil
import time
import uuid
from os.path import join, exists

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import udf
from pyspark.ml.recommendation import ALS
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import IntegerType


def train_recommender_system(df: DataFrame,
                             iterations: int = 10,
                             k_fold: bool = False,
                             logs: bool = False):
    starting__time = time.time()
    if logs:
        print(f"Starting training of ALS model")

    # instantiates the model
    als = ALS(maxIter=iterations, regParam=1e-2,
              userCol="user_id", itemCol="product_id", ratingCol="rating")

    if k_fold:
        # sets up k-fold cross validation
        param_grid = ParamGridBuilder() \
            .addGrid(als.rank, [10, 50, 100]) \
            .addGrid(als.regParam, [.01, .05, .1]) \
            .build()

        # train the model
        if logs:
            print("Models to be tested: ", len(param_grid))

        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction")

        cv = CrossValidator(estimator=als, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=3)

        # trains the model
        model = cv.fit(df).bestModel

    else:
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

    evaluator = RegressionEvaluator(metricName="rmse",
                                    labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(model.transform(df).na.drop())

    total_time = time.time() - starting__time
    if logs:
        print(f"Evaluation time: {int(total_time)}s")
        print(f"Root-mean-square error: {rmse}")


def save_model_zip(model,
                   output_folder=".",
                   model_name=None,
                   logs: bool = False):
    if not model_name:
        model_name = "model_" + str(uuid.uuid4()).replace("-", "")[:8]

    if exists(join(output_folder, model_name)):
        shutil.rmtree(join(output_folder, model_name))

    model.save(join(output_folder, model_name))

    shutil.make_archive(join(output_folder, model_name), 'zip', join(output_folder, model_name))

    shutil.rmtree(join(output_folder, model_name))

    if logs:
        print(f"Model saved at {join(output_folder, model_name + '.zip')}")

    return join(output_folder, model_name + '.zip')
