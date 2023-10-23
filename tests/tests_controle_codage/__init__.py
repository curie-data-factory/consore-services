# -*- coding: utf-8 -*-
"""
Created on 2023-02-28
@author: tbalezea
"""
import os

from util.util import ROOT_PATH

referentiels_path = (ROOT_PATH + "/tests/tests_controle_codage/referentiels/").replace(
    "\\", "/"
)
output_path = (ROOT_PATH + "/tests/tests_controle_codage/output/").replace("\\", "/")

referentiels_path

sys_argv = [
    "main.py",
    "--consore",
    "consore.json",
    "--inputkeywords",
    output_path + "keywords.xlsx",
    "--datedeb",
    "2022-01-01",
    "--datefin",
    "2022-01-03",
    "--SANS_ATCD",
    "true",
    "--SANS_NEGATION",
    "true",
    "--SANS_HYPOTHESE",
    "true",
]
