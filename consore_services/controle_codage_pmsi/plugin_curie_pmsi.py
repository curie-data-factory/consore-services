import pandas as pd

from util.mysql_util import get_dataframe_from_query_string


def get_sejours_ghm(plugin_creds, datedeb, datefin):
    query = f"""
    SELECT DISTINCT code_visite,ghm_code,ghm_lib
            FROM data_pmsi
    WHERE sortie BETWEEN '{datedeb}' AND '{datefin}'

    """
    df = get_dataframe_from_query_string(
        database_creds=plugin_creds, query_string=query
    )

    df["code_severite"] = df.apply(
        lambda row: None if pd.isna(row["ghm_code"]) else row["ghm_code"][-1],
        axis=1,
    )

    return df
