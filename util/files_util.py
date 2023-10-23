"""Module de gestion des fichiers."""

import os

import pandas as pd


def get_content(path, encoding=None):
    """Lis un fichier selon son path.

    la fonction suivante retourne le contenu d'un fichier

    Parameters
    ----------
    path : String
        Chemin d'accès du fichier

    Returns
    -------
    text : String
        Contenue de fichier

    Reference
    ---------
    @tbalezea

    Examples
    --------
    >>> #TODO
    """
    if encoding is not None:
        with open(path, mode="r", encoding=encoding) as file:
            return file.read()
    else:
        with open(path, mode="r") as file:
            return file.read()


def create_path_and_put_content(
    filepath, content, openmode="w", raisemode=False, encode="utf-8"
):
    """Creer et insére du contenue sur un chemin dédier.

    la fonction suivante créer les repertoires parents du fichier [filepath]
    et insère le contenu [content] dans le document
    le mode d'ouverture doit être spécifié [openmode]

    w (écrase le fichier si existant)
    a (ajoute au fichier si existant)
    x (ne créer pas si le fichier éxiste déjà)

    Parameters
    ----------
    filepath : String
        Chemin d'accès du répértoire parent
    content : String
        Nom du fichier
    openmode : String
        Mode d'ouverture du fichier
    encode : String
        Encodage du fichier

    Returns
    -------
    None.

    Reference
    ---------
    @tbalezea

    Examples
    --------
    >>> #TODO
    """

    if not os.path.exists(os.path.dirname(filepath)):
        try:
            os.makedirs(os.path.dirname(filepath))
        except OSError as exc:
            if raisemode:
                if not os.path.exists(os.path.dirname(filepath)):
                    if exc.error != exc.error.EXIST:
                        raise

    with open(filepath, openmode, encoding=encode) as file:
        file.write(content)
