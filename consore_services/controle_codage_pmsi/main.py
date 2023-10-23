"""
Created on march 23 2023

@author: tbalezea
"""


import sys
import time
from datetime import datetime
from typing import Dict, List

import pandas as pd

from consore_services.controle_codage_pmsi.plugin_curie_pmsi import get_sejours_ghm
from consoretoolbox.consore import Consore
from util.creds_util import get_creds
from util.logger_util import get_module_logger

logger = get_module_logger("CONTROLE CODAGE PMSI")


def parse_keywords_file(file_path: str) -> pd.DataFrame:
    """
    Fonction qui récupere les mots clefs
    depuis le fichier excel fournit en entrée
    """
    keywords = pd.read_excel(file_path, sheet_name=0)

    return keywords


def get_sentences(
    consore_creds: dict,
    keyword_list: List[str],
    patient_list: List[str],
    tag_sans_param: List[str],
) -> pd.DataFrame:
    """
    Fonction qui récupere les phrases
    à partir de mots clefs et d'une liste de patient
    """
    cons = Consore(consore_creds)
    cons.connect()
    # SANS_HYPOTHESE,SANS_HEADER,SANS_FAMILIAL
    df_sentences = cons.sentences_from_keywords_and_patients(
        keyword_list=keyword_list,
        patient_list=patient_list,
        source_filter=["CR"],
        tag_sans=tag_sans_param,
    )
    return df_sentences


def comparecodes(pmsicodes: str, keywordcodes: str) -> bool:
    """
    Compare les codes diagnostics
    """
    if not pd.isna(pmsicodes):
        pmsi_lst = pmsicodes.split(",")
        keyword_lst = keywordcodes.split(",")

        for code in keyword_lst:
            if code in pmsi_lst:
                return True

    return False


def get_pmsi_data(
    consore_creds: dict, date_deb: str, date_fin: str, patient_ids: List[str] = None
) -> pd.DataFrame:
    cons = Consore(consore_creds)
    cons.connect()

    sejours_df = cons.getpmsidata(
        date_deb=date_deb,
        date_fin=date_fin,
        patient_ids=patient_ids,
        date_field="dateFin",
    )

    sejours_df["entreele"] = pd.to_datetime(sejours_df["entreele"]).dt.date
    sejours_df["sortie"] = pd.to_datetime(sejours_df["sortie"]).dt.date

    # on filtre les séjours inférieur à deux jours
    difference = sejours_df["sortie"] - sejours_df["entreele"]
    sejours_df = sejours_df[difference > pd.Timedelta(days=2)]
    # on filtre les séjours de radiothérapies
    sejours_df = sejours_df[~sejours_df["actes"].str.contains("ZZNL")]
    sejours_df = sejours_df[~sejours_df["actes"].str.contains("ZZMK")]
    sejours_df = sejours_df[~sejours_df["diagcodes"].str.contains("Z51.0")]
    sejours_df = sejours_df.drop("actes", axis=1)

    return sejours_df


def process_main():
    """
    Main function
    """

    # récupération des arguments de la ligne de commandes
    dict_arg = {}
    sys_argv = sys.argv

    i = 1  # On commence à 1 pour éviter de vérifier sys_argv[0]
    while i < len(sys_argv):
        arg = sys_argv[i]

        if arg.startswith("--") and i + 1 < len(sys_argv):
            argkey = arg[2:]  # Équivalent à arg.replace("--", "")
            argval = sys_argv[i + 1]
            dict_arg[argkey] = argval
            i += 2  # Avance de 2 pour sauter le nom de l'argument et sa valeur
        else:
            # Gère les cas où l'argument ne commence pas par "--" ou il manque une valeur
            logger.info(f"Argument invalide : {arg}")
            i += 1

    consore_creds = get_creds(dict_arg["consore"])

    plugin_creds = None
    if "plugin_db" in dict_arg:
        plugin_creds = get_creds(dict_arg["plugin_db"])

    input_keywords_path = dict_arg["inputkeywords"]
    datedeb = dict_arg["datedeb"]
    datefin = dict_arg["datefin"]

    # Convertir les chaînes en objets date
    date_debut = datetime.strptime(datedeb, "%Y-%m-%d")
    date_fin = datetime.strptime(datefin, "%Y-%m-%d")
    # Calculer la différence entre les deux dates
    difference = date_fin - date_debut

    if difference.days > 365:
        raise Exception("la borne de date trop élevée : 1 an maximum ")

    tag_sans_lst = ["SANS_ATCD", "SANS_NEGATION", "SANS_HYPOTHESE"]
    tag_sans_param = []
    for tag_sans in tag_sans_lst:
        if tag_sans in dict_arg and dict_arg[tag_sans] == "true":
            tag_sans_param.append(tag_sans)

    logger.info("GET SEJOURS ... ")
    # Get sejours and diags from pmsi

    sejours_df = get_pmsi_data(
        consore_creds=consore_creds, date_deb=datedeb, date_fin=datefin
    )

    if "plugin_db" in dict_arg:
        # plugin curie

        ghm_df = get_sejours_ghm(
            plugin_creds=plugin_creds, datedeb=datedeb, datefin=datefin
        )

        sejours_df = sejours_df.merge(ghm_df, how="left", on="code_visite")

    else:
        sejours_df["ghm_code"] = None
        sejours_df["ghm_lib"] = None
        sejours_df["code_severite"] = None

    patient_list = sejours_df["patient_ipp"].unique().tolist()

    # Obtenir la liste des mots clefs depuis le fichier d'entrée
    keywords_df = parse_keywords_file(file_path=input_keywords_path)
    keyword_list = keywords_df["keyword"].unique().tolist()

    logger.info("GET SENTENCES ... ")
    # query ES consore pour obtenir les phrases qui matche les keywords
    sentences_df = get_sentences(
        consore_creds=consore_creds,
        patient_list=patient_list,
        keyword_list=keyword_list,
        tag_sans_param=tag_sans_param,
    )

    # formattage des dates
    sentences_df["dateDebut"] = pd.to_datetime(sentences_df["dateDebut"]).dt.date

    logger.info("MERGED SEJOURS AND SENTENCES ... ")
    mergedf = sejours_df.merge(
        sentences_df, how="left", left_on="patient_ipp", right_on="ipp"
    )

    logger.info("PROCESSING DATA ... ")
    # on positionne les documents dans les séjours ( date_doc entre debut et fin séjour)
    # règle spéciale pour les doc d'anesthesie:
    # on autorise de matcher  les documents 3 mois avant la date de début de séjour
    mergedf_filter = mergedf[
        (
            (mergedf["dateDebut"] >= mergedf["entreele"])
            & (mergedf["dateDebut"] <= mergedf["sortie"])
        )
        | (
            (mergedf["type"] == "CR d'anesthésie")
            & (mergedf["dateDebut"] <= mergedf["sortie"])
            & (mergedf["dateDebut"] >= (mergedf["entreele"] + pd.DateOffset(days=-90)))
        )
    ]

    # Assemblage des  (sejours + phrases match keywords )
    # avec les keywords et leurs correspondances cim10
    mergedf_filter = mergedf_filter.merge(
        keywords_df, how="inner", left_on="keyword", right_on="keyword"
    )

    # Comparaison des codes cim10
    mergedf_filter["alreadyexist"] = mergedf_filter.apply(
        lambda row: comparecodes(pmsicodes=row["diagcodes"], keywordcodes=row["codes"]),
        axis=1,
    )

    # On ne garde que les lignes dont les mots clefs dans les phrases
    # ne correspondent pas a des codes cim dans les sejours pmsi
    mergedf_filter = mergedf_filter[mergedf_filter["alreadyexist"] == False]

    # Filrage des colonnes non essentielles
    mergedf_filter = mergedf_filter.drop(
        [
            "diagcodes",
            "service",
            "ipp",
            "source",
            "patient_ipp_y",
            "sentence_order",
            "offset",
            "alreadyexist",
            "tagSans",
        ],
        axis=1,
    )

    lien_consore = f'https://{consore_creds["host"]}/group/guest/document/-/contenu/-/'
    mergedf_filter["consore_link"] = mergedf_filter["id"].map(
        lambda x: lien_consore + x
    )

    # Création de la sortie excel
    path_output = (
        input_keywords_path[: input_keywords_path.rindex(".")] + "_output.xlsx"
    )
    mergedf_filter["entreele"] = pd.to_datetime(
        mergedf_filter["entreele"].astype(str), format="%Y-%m-%d"
    ).dt.strftime("%d/%m/%Y")
    mergedf_filter["sortie"] = pd.to_datetime(
        mergedf_filter["sortie"].astype(str), format="%Y-%m-%d"
    ).dt.strftime("%d/%m/%Y")
    mergedf_filter["dateDebut"] = pd.to_datetime(
        mergedf_filter["dateDebut"].astype(str), format="%Y-%m-%d"
    ).dt.strftime("%d/%m/%Y")
    mergedf_filter["code_visite"] = mergedf_filter["code_visite"].astype(str)

    mergedf_filter.rename(columns={"dateDebut": "date_doc"}, inplace=True)

    with pd.ExcelWriter(path_output) as writer:
        mergedf_filter.to_excel(writer, sheet_name="Description Variables", index=False)


if __name__ == "__main__":
    # *****************************
    # EXEMPLE de ligne de commande :
    # --consore consore.json  --plugin_db plugindb.json  --inputkeywords C:/Users/tbalezea/PycharmProjects/controle-codage-pmsi/tests/tests_controle_codage/output/keywords.xlsx  --datedeb 2021-01-01  --datefin 2021-03-01
    # Options possible :
    # --SANS_ATCD true --SANS_NEGATION true --SANS_HYPOTHESE false
    # --plugin_db plugindb.json
    # *****************************
    process_main()
