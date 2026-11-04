from pyspark.sql.functions import to_date, expr, when, cast

def at_hhmm(date_col: str, hhmm_col_name: str):
    """
    Return a TIMESTAMP_NTZ column with the date from date_col and the time from hhmm_col_name.
    This is a local time, not a UTC time, and does not include a time zone.

    The HHmm string is assumed to be in the same time zone as the date_col.

    Parameters
    ----------
    date_col: str
        A column containing a date in the format 'yyyy-MM-dd'.
    hhmm_col_name: str
        A column containing a time in the format 'HHmm'.

    Returns
    -------
    A TIMESTAMP_NTZ column with the date from date_col and the time from hhmm_col_name

    Example:
      df = spark.createDataFrame(
        [
            ("2022-01-01", "0000"),
            ("2022-01-01", "0001"),
            ("2022-01-01", "2359"),
        ],
        ["date", "hhmm"],
    )
    df.select(at_hhmm("date", "hhmm")).show()
    +-------------------+
    |at_hhmm(date, hhmm)|
    +-------------------+
    |     2022-01-01 00:00:00|  
    |     2022-01-01 00:01:00|  
    |     2022-01-01 23:59:00|  
    +-------------------+
    """
    # date + HH hours + mm minutes; make_dt_interval rolls '2400' over to 00:00 of the next day
    return to_date(date_col).cast("timestamp_ntz") + expr(
        f"make_dt_interval(0, int(substr({hhmm_col_name}, 1, 2)), int(substr({hhmm_col_name}, 3, 2)), 0)"
    )

def plus_minutes(ts_col: str, minutes_col_name: str):
    """
    Return a TIMESTAMP_NTZ column with the timestamp from ts_col plus the number of minutes from minutes_col_name.
    This is a local time, not a UTC time, and does not include a time zone.

    Parameters
    ----------
    ts_col: str
        A column containing a timestamp in the format 'yyyy-MM-dd HH:mm:ss'.
    minutes_col_name: str
        A column containing a number of minutes.

    Returns
    -------
    A TIMESTAMP_NTZ column with the timestamp from ts_col plus the number of minutes from minutes_col_name

    Example:
      df = spark.createDataFrame(
        [
            ("2022-01-01 00:00:00", "0000"),
            ("2022-01-01 00:00:00", "0001"),
            ("2022-01-01 00:00:00", "2359"),
        ],
        ["ts", "minutes"],
    )
    df.select(plus_minutes("ts", "minutes")).show()
    +-------------------+
    |plus_minutes(ts, minutes)|
    +-------------------+
    |     2022-01-01 00:00:00|  
    |     2022-01-01 00:01:00|  
    |     2022-01-01 23:59:00|  
    +-------------------+
    """
    return ts_col + expr(f"make_dt_interval(0, 0, int(cast({minutes_col_name} as double)), 0)")

def arrival_near(departure_ts: str, hhmm_col_name: str):
    """
    Return a TIMESTAMP_NTZ column with the arrival clock time placed on the departure date, then shifted by one day if needed.
    This is a local time, not a UTC time, and does not include a time zone.

    The HHmm string is assumed to be in the same time zone as the departure_ts.

    local arrival - local departure = flight duration + time zone difference, which is within [-6h, +18h] for US flights

    Parameters
    ----------
    departure_ts: str
        A column containing a timestamp in the format 'yyyy-MM-dd HH:mm:ss'.
    hhmm_col_name: str
        A column containing a time in the format 'HHmm'.

    Returns
    -------
    A TIMESTAMP_NTZ column with the arrival clock time placed on the departure date, then shifted by one day if needed.

    Example:
      df = spark.createDataFrame(
        [
            ("2022-01-01 00:00:00", "0000"),
            ("2022-01-01 00:00:00", "0001"),
            ("2022-01-01 00:00:00", "2359"),
        ],
        ["departure_ts", "hhmm"],
    )
    df.select(arrival_near("departure_ts", "hhmm")).show()
    +-------------------+
    |arrival_near(departure_ts, hhmm)|
    +-------------------+
    |     2022-01-01 00:00:00|  
    |     2022-01-01 00:01:00|  
    |     2022-01-01 23:59:00|  
    +-------------------+
    """
    naive = at_hhmm(departure_ts, hhmm_col_name)
    return (
        when(naive < departure_ts - expr("INTERVAL 6 HOURS"), naive + expr("INTERVAL 1 DAY"))
        .when(naive > departure_ts + expr("INTERVAL 18 HOURS"), naive - expr("INTERVAL 1 DAY"))
        .otherwise(naive)
    )

def nearest_to(ref_ts: str, hhmm_col_name: str):
    """
    Return a TIMESTAMP_NTZ column with the clock time placed on whichever day (previous / same / next) is within 12 hours of a reference timestamp.
    This is a local time, not a UTC time, and does not include a time zone.

    The HHmm string is assumed to be in the same time zone as the ref_ts.

    Parameters
    ----------
    ref_ts: str
        A column containing a timestamp in the format 'yyyy-MM-dd HH:mm:ss'.
    hhmm_col_name: str
        A column containing a time in the format 'HHmm'.

    Returns
    -------
    A TIMESTAMP_NTZ column with the clock time placed on whichever day (previous / same / next) is within 12 hours of a reference timestamp.

    Example:
      df = spark.createDataFrame(
        [
            ("2022-01-01 00:00:00", "0000"),
            ("2022-01-01 00:00:00", "0001"),
            ("2022-01-01 00:00:00", "2359"),
        ],
        ["ref_ts", "hhmm"],
    )
    df.select(nearest_to("ref_ts", "hhmm")).show()
    +-------------------+
    |nearest_to(ref_ts, hhmm)|
    +-------------------+
    |     2022-01-01 00:00:00|  
    |     2022-01-01 00:01:00|  
    |     2022-01-01 23:59:00|  
    +-------------------+
    """    
    naive = at_hhmm(ref_ts, hhmm_col_name)
    return (
        when(naive < ref_ts - expr("INTERVAL 12 HOURS"), naive + expr("INTERVAL 1 DAY"))
        .when(naive > ref_ts + expr("INTERVAL 12 HOURS"), naive - expr("INTERVAL 1 DAY"))
        .otherwise(naive)
    )
