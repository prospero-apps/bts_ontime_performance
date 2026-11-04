from databricks.sdk.runtime import spark

def initialize_lookup_table(filename: str, table_name: str):
    """
    Initialize a lookup table from a csv file.

    Parameters
    ----------
    filename : str
        The name of the csv file.
    table_name : str
        The name of the table to create.

    Returns
    -------
    None
    """
    path = f"/Volumes/bts_ontime/00_landing/data_sources/lookups/{filename}.csv"
    df = spark.read.format("csv").option("header", "true").load(path)
    df.write.mode("overwrite").saveAsTable(f"bts_ontime.01_bronze.{table_name}")
