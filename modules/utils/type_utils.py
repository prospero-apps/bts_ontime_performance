from pyspark.sql.functions import col, cast
from pyspark.sql.types import DoubleType, IntegerType

def to_int(col_name: str):
    """
    Convert a column to int. Source values look like '125.00'; ANSI mode rejects a direct string -> int cast, so go via double.

    Parameters:
        col_name (str): name of the column to convert
    Returns:
        Column
    Example:
        to_int('col1')
    """
    return col(col_name).cast(DoubleType()).cast(IntegerType())

def to_bool(col_name: str):
    """
    Convert a column to boolean. Source flags are '0.00' / '1.00' -> false / true; ANSI mode rejects a direct string -> boolean cast, so go via double.

    Parameters:
        col_name (str): name of the column to convert
    Returns:
        Column
    Example:
        to_bool('col1')
    """
    return col(col_name).cast(DoubleType()) == 1.0