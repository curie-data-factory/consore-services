# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 11:13:57 2022

@author: vnguyen
"""

import sys
from urllib.parse import quote

import pandas as pd
from sqlalchemy import create_engine

from util import files_util
from util.logger_util import get_module_logger

logger = get_module_logger("MYSQL_UTIL")
logger_data_lineage = get_module_logger("MYSQL_UTIL_DATA_LINEAGE")


def get_url_connexion(sql_config, other_params=None):
    """Fonction qui va créer l'URL de connexion pour une base de donnée mysql .

    Elle va générer une url selon la configuration de la base et selon le type
    de base de données

    Parameters
    ----------
    basetype : String
        Type de base de données
    sql_config : Dict
        Dictionnaire de creds
    other_params : Dict, optional
        Parametre supplementaire à la connexion. The default is None.

    Returns
    -------
    url_sql : String
        URL de connexion à la base de donnees

    References
    ----------
    vnguyen



    """
    config = sql_config
    print(config)
    url_sql = ""

    keywords = ["user", "password", "host", "port", "database"]
    diff = list(set(keywords) - set(list(config.keys())))
    if len(diff) == 0:
        basetype = "mysql+mysqldb"
        url_base_user = f"{basetype}://{config['user']}:{quote(config['password'])}"
        url_sql = f"{url_base_user}@{config['host']}:{str(config['port'])}"
        if "database" in config:
            url_sql = f"{url_sql}/{config['database']}"
    else:
        error = "; ".join(diff)
        logger.error("Their some information missing : %s", error)
        sys.exit(1)

    if other_params is not None:
        params = []

        for i in other_params:
            params.append(i + "=" + other_params[i])

        url_params = "?" + "&".join(params)
        url_sql = url_sql + url_params

    return url_sql


def get_connexion(mysql_config):
    """Créer une connexion à une base MySql.

    Cette fonction fourni la connexion a une base de donnees MySQL, MariaDB
    a partir des credentials de la class Arguments

    Parameters
    ----------
    mysql_config : Dict
        Dictionnaire de creds
    other_params : Dict
        Dictionnaire de paramètre si l'utilisateur possède des spécification
        au niveau de la connection

    Returns
    -------
    sqlalchemy.engine
        Connexion mysql

    References
    ----------
    vnguyen


    """
    url_sql = get_url_connexion(mysql_config)

    try:
        remote_database = create_engine(url_sql).connect()
        return remote_database

    except:
        logger.error("The connection can't be create")
        raise


def get_dataframe_from_query_file(
    database_creds, path_file, params_mapping=None, verbose=True
):
    """Récupère des données depuis un fichier sql.

    Cette fonction execute une requete sur une base mysql de type SELECT
    et renvoie les resultats sous forme de dataframe.
    Le parametre optionnel params_mapping permet de remplacer
    dans la requete un pattern par une valeur

    Parameters
    ----------
    database_creds : String
      La configuration de la base de données
    query_string : String
        requette à executer
    params_mapping : dict {string:string}
      dictionnaire de remplacement clef valeur

    Returns
    -------
    df_query : pd.DataFrame
        Données récupérés via la query dans le chemin path_file

    References
    ----------
    tbalezea


    """
    query_string = files_util.get_content(path_file)
    df_query = get_dataframe_from_query_string(
        database_creds, query_string, params_mapping, verbose
    )
    return df_query


def get_dataframe_from_query_string(database_creds, query_string):
    """Récupère des données selon une requête sql.

    Cette fonction execute une requete sur une base mysql de type SELECT
    et renvoie les resultats sous forme de dataframe
    le parametres optionnel params_mapping permet de remplacer
    dans la requete un pattern por une valeur

    Parameters
    ----------
    database_creds : String
      La configuration de la base de données
    path_file : String
        chemin du fichier contenant la requete
    params_mapping : dict {string:string}
      dictionnaire de remplacement clef valeur

    Returns
    -------
    df_query : pd.DataFrame.
        Données récupérés via la query_string

    References
    ----------
    tbalezea

    """

    connect = get_connexion(database_creds)
    query_string = query_string.replace("%", "%%")
    try:
        df_query = pd.read_sql_query(query_string, connect)
        df_query = df_query.convert_dtypes()
        connect.detach()
        connect.close()

    except Exception as show_error:
        connect.detach()
        connect.close()
        raise Exception(f"Error getting dataframe query : {show_error}")

    return df_query
