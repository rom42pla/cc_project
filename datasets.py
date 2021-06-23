from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


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


def remapping(df: DataFrame, column: str):
    # maps all the values
    unique_values = [row[column] for row in df.select(column).distinct().collect()]
    remapping_dict = {
        unique_value: i + 1
        for i, unique_value in enumerate(unique_values)
    }
    # replaces the values in column
    df = df.withColumn(column,
                       udf(lambda value: remapping_dict[value], IntegerType())(df[column]))
    return df


def preprocess_dataframe(df: DataFrame):
    df = df.select("event_time", "user_id", "event_type", "category_id", "product_id")
    df = remapping(df, "product_id")
    df = remapping(df, "category_id")
    df = remapping(df, "user_id")
    df = assign_rating_from_views(df)
    return df