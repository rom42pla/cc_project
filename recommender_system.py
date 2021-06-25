import itertools
import shutil
import time
import uuid
from os.path import join, exists

import numpy as np

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql.dataframe import DataFrame


def train_recommender_system(df_train: DataFrame, df_test: DataFrame,
                             max_iter: int = 10, rank: int = 10, reg_param=0.1,
                             tuning: bool = False,
                             logs: bool = False):
    starting__time = time.time()
    model = None

    # tries several hyperparameters for tuning
    if tuning:
        best_model, best_rmse = None, np.inf
        reg_params = [1, 0.1, 0.01]
        ranks = [5, 10, 50, 100]
        combinations = list(itertools.product(reg_params, ranks))
        for i_model, (reg_param, ranks) in enumerate(combinations):
            if logs:
                print(f"\ttraining model {i_model + 1}/{len(combinations)}")
            model = ALS(maxIter=max_iter, rank=rank, regParam=reg_param,
                        userCol="user_id", itemCol="product_id", ratingCol="rating").fit(df_train)
            rmse = evaluate_recommender_system(df=df_test, model=model, logs=False)
            # checks if a new best model is found
            if rmse < best_rmse:
                if logs:
                    print(f"\t\tfound best model with RMSE={rmse}:\n"
                          f"\t\t\trank={rank}\treg_param={reg_param}")
                best_model, best_rmse = model, rmse
            model = best_model
    # else fits the model
    else:
        model = ALS(maxIter=max_iter, rank=rank, regParam=reg_param,
                    userCol="user_id", itemCol="product_id", ratingCol="rating").fit(df_train)

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

    return rmse


def save_model_zip(model,
                   output_folder=".",
                   model_name=None,
                   logs: bool = False):
    # eventually invents a name for the file
    if not model_name:
        model_name = "model_" + str(uuid.uuid4()).replace("-", "")[:8]
    # removes previously saved models with same name
    if exists(join(output_folder, model_name)):
        shutil.rmtree(join(output_folder, model_name))
    # saves the model as a folder
    model.save(join(output_folder, model_name))
    # zips model's folder
    shutil.make_archive(join(output_folder, model_name), 'zip', join(output_folder, model_name))
    # removes folder of the model
    shutil.rmtree(join(output_folder, model_name))

    if logs:
        print(f"Model saved at {join(output_folder, model_name + '.zip')}")

    return join(output_folder, model_name + '.zip')
