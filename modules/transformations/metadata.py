from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

def add_processed_timestamp(df: DataFrame) -> DataFrame:
    """
    Add a column to the dataframe with the current timestamp.

    Parameters
    ----------
    df : DataFrame
        The dataframe to add the column to.

    Returns
    -------
    DataFrame
        The dataframe with the column added.
    """
    return df.withColumn("processed_timestamp", current_timestamp())