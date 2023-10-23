# -*- coding: utf-8 -*-
"""
Created on Tue Jul  5 09:51:48 2022

@author: vnguyen
"""
import logging


def get_module_logger(mod_name, package_name="PYTHONTOOLBOX"):
    """Fonction de parametrage des logs.

    Initialise un logger selon une certaine convention.
    date -package name - type of log - msg

    Parameters
    ----------
    mod_name : String
        Nom du module

    Returns
    -------
    logger : logging.logger
        Objet logger afin d'afficher une information

    References
    ----------
    @vnguyen

    Examples
    --------
    >>> logger = get_module_logger("my package")
    >>> logger.info("This is a information")
    2022-07-05 10:31:35,412 - my package - INFO - This is a information
    """
    if package_name is None:
        log_info = mod_name
    else:
        log_info = package_name + " - " + mod_name

    logger = logging.getLogger(log_info)
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
