import difflib
import glob
import os
import re

import pandas as pd

from util import files_util


def compare_strings(string1, string2, perc_diff_threshold=0):
    """
    cette fonction compare deux chaine de caractères
    elle renvoie les différences entre les deux chaines
    pour chaque différence on trouve la position de début de fin et la sous chaine
    on sait aussi si la différence est un ajout + ou une délétion - grace au champ diff_type
    :param string1:
    :param string2:
    :return: list[{"diff_type":"+" ou "-","string":string,"startidx":int,"endidx":int}]
    """
    currentdiff = {"diff_type": "", "string": "", "startidx": None, "endidx": None}

    diff_list = []
    for i, s in enumerate(difflib.ndiff(string1, string2)):
        if currentdiff["endidx"] is None:
            currentdiff["endidx"] = i
        if s[0] not in ["-", "+"]:
            continue
        else:
            print(s + " " + str(i))
            # print(currentdiff["diff_type"] +" "+ s[0])
            if currentdiff["endidx"] + 1 < i:
                if currentdiff["diff_type"] in ["-", "+"]:
                    diff_list.append(currentdiff.copy())
                currentdiff["diff_type"] = s[0]
                currentdiff["string"] = s[-1]
                currentdiff["startidx"] = i
                currentdiff["endidx"] = i + 1

            else:
                currentdiff["string"] += s[-1]
                # currentdiff["startidx"] += i
                currentdiff["endidx"] += 1
            # print(currentdiff)

    if currentdiff["diff_type"] in ["+", "-"]:
        diff_list.append(currentdiff.copy())

    totaldiff = 0
    for currentdiff in diff_list:
        totaldiff = totaldiff + len(currentdiff["string"])

    print(totaldiff)
    percent_diff = totaldiff * 100 / len(string1)

    if percent_diff > perc_diff_threshold:
        assert len(diff_list) == 0, "strings diff :\n" + str(diff_list)

    return diff_list


def compare_dataframe(df1, df2):
    """
    cette fonction compare deux dataframes
    et renvoie dans un objet les différences de noms de colonnes
    et de valeures au sein du dataframe

    :param df1: dataframe
    :param df2: dataframe
    :return: {"diff_col": list[string], "diff_val":df_compare}
    """
    df1col = df1.columns.tolist()
    df2col = df2.columns.tolist()

    df1miss = [i for i in df1 if i not in df2]
    df2miss = [i for i in df2 if i not in df1]

    col_diff = df1miss + df2miss

    # print("diff columns : "+str(df1miss+df2miss))
    for col in df1miss:
        del df1[col]
    for col in df2miss:
        del df2[col]

    df_compare = df1.compare(df2)
    return_obj = {"diff_col": col_diff, "diff_val": df_compare}

    assert len(col_diff) == 0, "diff columns :\n" + str(col_diff)
    assert len(df_compare) == 0, "diff values :\n" + df_compare.to_string()

    return return_obj


def compare_directory(referentiels_path, output_path, perc_diff_threshold=10):
    """
    Fonction qui compare les données référentiels et les données de sortie du programme
    """

    referentiels_path = referentiels_path.replace("\\", "/")
    output_path = output_path.replace("\\", "/")

    if referentiels_path[-1] != "/":
        referentiels_path = referentiels_path + "/"
        output_path = output_path + "/"

    ref_files = [
        file.replace("\\", "/").replace(referentiels_path, "")
        for file in glob.glob(referentiels_path + "**", recursive=True)
        if not os.path.isdir(file)
    ]
    out_files = [
        file.replace("\\", "/").replace(output_path, "")
        for file in glob.glob(output_path + "**", recursive=True)
        if not os.path.isdir(file)
    ]

    for file in ref_files:
        if file not in out_files:
            assert False, f"""le fichier {file}  n'est pas present dans la sortie"""

        else:
            # comparaison des sorties
            output_file_path = output_path + file
            ref_file_path = referentiels_path + file

            if re.search("\\.csv", file):
                ref_df = pd.read_csv(ref_file_path, dtype=object)
                output_df = pd.read_csv(output_file_path, dtype=object)
                compare_dataframe(ref_df, output_df)
            elif re.search("\\.xlsx", file):
                ref_sheet_names = pd.ExcelFile(ref_file_path).sheet_names
                output_sheet_names = pd.ExcelFile(output_file_path).sheet_names
                for sheet in ref_sheet_names:
                    if sheet not in output_sheet_names:
                        assert (
                            False
                        ), f"""le feuille {sheet}  n'est pas present dans la sortie"""
                    else:
                        ref_df = pd.read_excel(
                            ref_file_path,
                            dtype=object,
                            engine="openpyxl",
                            sheet_name=sheet,
                        )
                        output_df = pd.read_excel(
                            output_file_path,
                            dtype=object,
                            engine="openpyxl",
                            sheet_name=sheet,
                        )
                        compare_dataframe(ref_df, output_df)
            else:
                ref_string = files_util.get_content(path=ref_file_path)
                output_string = files_util.get_content(path=output_file_path)
                compare_strings(string1=ref_string, string2=output_string)
