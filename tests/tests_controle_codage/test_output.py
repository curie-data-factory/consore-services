# -*- coding: utf-8 -*-
"""
Created on 2023-10-01

@author: thomas balezeau
"""

import os
import shutil
import sys
from unittest.mock import patch

import pandas as pd

from consore_services.controle_codage_pmsi.main import (
    get_pmsi_data,
    get_sentences,
    parse_keywords_file,
    process_main,
)
from util import compare_util, logger_util
from util.creds_util import get_creds

from . import output_path, referentiels_path, sys_argv

logger = logger_util.get_module_logger("Test controle codage", "Test")
# pytest -vvv -s tests/tests_controle_codage


@patch("consore_services.controle_codage_pmsi.main.get_sentences")
@patch("consore_services.controle_codage_pmsi.main.get_pmsi_data")
def test_compare_directory(mock_sejours, mock_sentences):
    """
    Fonction qui compare les données référentiels et les données de sortie du programme
    """
    sejours_df = pd.read_csv(
        referentiels_path + "mock_data/sejours.csv", sep=";", index_col=False
    )
    sejours_df["entreele"] = pd.to_datetime(sejours_df["entreele"]).dt.date
    sejours_df["sortie"] = pd.to_datetime(sejours_df["sortie"]).dt.date
    mock_sejours.return_value = sejours_df

    mock_sentences.return_value = pd.read_csv(
        referentiels_path + "mock_data/sentences.csv", sep=";", index_col=False
    )

    sys.argv = sys_argv

    process_main()

    ref_df = pd.read_excel(
        referentiels_path + "keywords_output.xlsx",
        dtype=object,
        engine="openpyxl",
        sheet_name="Description Variables",
    )
    output_df = pd.read_excel(
        output_path + "keywords_output.xlsx",
        dtype=object,
        engine="openpyxl",
        sheet_name="Description Variables",
    )
    compare_util.compare_dataframe(ref_df, output_df)
