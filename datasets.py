from typing import Union, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, LongType


def assign_rating_from_views(df: DataFrame):
    purchases_users = [row["user_id"]
                       for row in df.where(df.event_type == "purchase").select("user_id").collect()]
    rating_dict = {
        (row["user_id"], row["product_id"]): row["count"]
        for row in df[df.user_id.isin(purchases_users)].where(df.event_type == "view")
            .groupby("user_id", "product_id").count().collect()
    }
    df = df.where(df.event_type == "purchase")
    df = df.withColumn("rating",
                       udf(lambda user_id, product_id:
                           rating_dict[(user_id, product_id)]
                           if (user_id, product_id) in rating_dict
                           else 0, IntegerType())
                       (df["user_id"], df["product_id"]))
    df = df.drop("event_type")
    return df


def remapping(df: DataFrame, columns: Union[str, List[str]]):
    # eventually casts the columns to a list
    if isinstance(columns, str):
        columns = [columns]
    # collect infos for each row
    # rows_list = [
    #     [row[column] for column in columns]
    #     for row in df.select(columns).collect()
    # ]
    for i_column, column in enumerate(columns):
        df = df.withColumn(column, df[column].cast(IntegerType()))
        min_value = df.agg({column: "min"}).collect()[0][0]
        # unique_values = {row[i_column] for row in rows_list}

        # maps each value to a number
        # remapping_dict = {
        #     unique_value: i + 1
        #     for i, unique_value in enumerate(unique_values)
        # }
        # replaces the values in column
        df = df.withColumn(column,
                           udf(lambda value: value - min_value, IntegerType())(df[column]))
    return df


def preprocess_dataframe(df: DataFrame):
    df = df.select("event_time", "user_id", "event_type", "product_id")
    df = remapping(df, ["product_id", "user_id"])
    df = assign_rating_from_views(df)
    return df
