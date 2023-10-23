# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-
"""
Created on Wed Dec 28 10:55:03 2022

@author: alegros
"""
import urllib.parse
import warnings
from time import time

import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import FunctionScore, Q

warnings.filterwarnings("ignore")


class Consore:
    """classe Consore"""

    def __init__(self, credentials):
        self.credentials = credentials
        self.client = None

    def connect(self):
        """connection a elasticsearch"""
        user = urllib.parse.quote(self.credentials["user"])
        password = urllib.parse.quote(self.credentials["password"])
        host = self.credentials["host"]
        port = self.credentials["port"]
        url = "https://" + user + ":" + password + "@" + host + ":" + port
        self.client = Elasticsearch(
            [url], verify_certs=False, timeout=100, max_retries=5, retry_on_timeout=True
        )

    def sentences_from_keywords_and_patients(
        self,
        keyword_list=None,
        patient_list=None,
        patient_windows_min=None,
        patient_windows_max=None,
        source_filter=["CR"],
        type_filter=None,
        ids_filter=None,
        tag_avec=None,
        tag_sans=None,
        nbsentences=None,
        batch_size=1000,
        index_context="current",
    ):
        """
        Méthode qui récupére toutes les phrases de l index Consore
        pour une liste ou une fenêtre de patients
        en fonction d une liste de mots-clés.

        Parameters
        ----------
        keyword_list : list
            Liste de mots-clés au format query string de ELS.
        patient_list : list
            Liste d identifiants patient (ipp).
        patient_windows_min : int
            Identifiant patient (ipp)
        patient_windows_max : int
            Identifiant patient (ipp)
        source_filter : list
            Liste de source de document en format string
        type_filter : list
            Liste de type de document en format string
        ids_filter : list
            Liste de ids de document en format string à filtrer
        tag_avec : list
            Liste de tag avec de document en format string
        tag_sans : list
            Liste de tag sans de document en format string
        nbsentences : int
            Nombre de phrases à retourner dans le dataframe
        batch_size : int
            Nombre de documents à requêter à chaque itération du scan.
            Attention un batch_size > 1000 peut générer des timeout error.
        index_context:string
            La recherche se fait sur les index consorepatient concaténés
            avec index_context. Par défaut on prend l index actuel.

        Returns
        -------
        df : pandas.DataFrame
            Dataframe contenant les phrases des documents indexés
            dans Consore par identifiant patient.

        """

        if keyword_list is None:
            keyword_list = []

        mydata = []

        if nbsentences is None:
            nbsentences = 1000000

        for index, keyword in enumerate(keyword_list):
            print(f"\nkeyword : {keyword}")

            # --MUST--

            ## Patient

            if (
                patient_list is None
                and patient_windows_min is None
                and patient_windows_max is None
            ):
                q_ipp_windows = Q()
                q_ipp_list = Q()

            else:
                if patient_list is None:
                    patient_list = [""]

                if patient_windows_min is None or patient_windows_max is None:
                    patient_windows_min = ""
                    patient_windows_max = ""

                q_ipp_windows = Q(
                    "range",
                    ipp={
                        "gte": patient_windows_min,
                        "lte": patient_windows_max,
                        "boost": 2.0,
                    },
                )
                q_ipp_list = Q("terms", ipp=patient_list, boost=1.0)

            q_ipp = q_ipp_windows | q_ipp_list

            ## Document Source_type = "CR" etc.
            sub_q_source = Q("terms", source=source_filter, boost=1.0)
            q_source = Q(
                "bool", should=[sub_q_source], adjust_pure_negative=True, boost=1.0
            )

            q_type = Q()

            if type_filter is not None and len(type_filter) > 0:
                sub_q_type = Q("terms", type__libelle=type_filter, boost=1.0)
                q_type = Q(
                    "bool", should=[sub_q_type], adjust_pure_negative=True, boost=1.0
                )

            if ids_filter is not None and len(ids_filter) > 0:
                sub_q_ids = Q("terms", id=ids_filter, boost=1.0)

                q_type = Q(
                    "bool", should=[sub_q_ids], adjust_pure_negative=True, boost=1.0
                )

            # --FILTER--

            ## Keyword
            sub_q_string = Q(
                "simple_query_string",
                query=keyword,
                fields=["phrases.texte^1.0"],
                analyzer="default",
                flags=-1,
                default_operator="and",
                analyze_wildcard=True,
                auto_generate_synonyms_phrase_query=True,
                fuzzy_prefix_length=0,
                fuzzy_max_expansions=50,
                fuzzy_transpositions=True,
                boost=1.0,
            )

            q_must = [sub_q_string]

            ## Tags
            if tag_avec is not None and len(tag_avec) > 0:
                for tag_a in tag_avec:
                    q_must.append(Q("terms", phrases__tagAvec=[tag_a], boost=1.0))

            if tag_sans is not None and len(tag_sans) > 0:
                for tag_s in tag_sans:
                    q_must.append(Q("terms", phrases__tagSans=[tag_s], boost=1.0))

            ## Must Keyword + Tags
            q_string = Q("bool", must=q_must, adjust_pure_negative=True, boost=1.0)

            ## Nested
            inner_hits = {
                "_source": [
                    "phrases.texte",
                    "phrases.type",
                    "phrases.ordre",
                    "phrases.tagSans",
                ],
                # "highlight":{"fields":{"phrases.texte":{}}}
            }

            q_filter = Q(
                "nested",
                query=q_string,
                path="phrases",
                ignore_unmapped=False,
                score_mode="avg",
                boost=1.0,
                inner_hits=inner_hits,
            )

            query = Q("bool", must=[q_ipp, q_source, q_type], filter=[q_filter])

            # Global Query with Function Score -> score_mode="avg"
            functionscore = FunctionScore(query=query)

            # Fiels to return
            fields = [
                "dateDebut",
                "service",
                "source",
                "titre",
                "ipp",
                "id",
                "type.libelle",
                "sousType.libelle",
                "providerOrganisation",
            ]

            response = (
                Search(using=self.client, index="consoredocument" + index_context)
                .query(functionscore)
                .source(fields)
            )
            total = response.params(size="1", request_timeout=50).execute()
            total_hits = total.hits.total
            print("total : ", total_hits)

            ## Warning : batch_size > 1000 returns timed out error
            scan = response.params(scroll="10m", size=batch_size).scan()
            current_nb_sentences = 0

            for n, hit in enumerate(scan):
                if nbsentences:
                    total_hits = nbsentences
                    if n + 1 > nbsentences:
                        break

                if n == 0:
                    start = time()
                    print("start collecting data...")

                if n == batch_size:
                    end = time()
                    batch_time = end - start
                    batch_qty = int(total_hits / batch_size)
                    total_time = int(batch_time * batch_qty)
                    print(f"time estimation : {total_time} seconds")

                add_mydatatemplate = hit.to_dict()

                inner_hits = hit.meta.to_dict()["inner_hits"]["phrases"].hits.hits

                break_scan = False

                if break_scan:
                    break

                for phrase_hit in inner_hits:
                    current_nb_sentences += 1

                    if current_nb_sentences > nbsentences:
                        break_scan = True

                    add_mydata = add_mydatatemplate.copy()
                    add_mydata["keyword"] = keyword
                    add_mydata["patient_ipp"] = add_mydata["ipp"]
                    add_mydata["type"] = add_mydata["type"]["libelle"]
                    add_mydata["sousType"] = add_mydata["sousType"]["libelle"]
                    add_mydata["sentence"] = phrase_hit["_source"]["texte"]
                    add_mydata["tagSans"] = phrase_hit["_source"]["tagSans"]
                    add_mydata["sentence_order"] = phrase_hit["_source"]["ordre"]
                    # add_mydata["sentence_highlighted"] = phrase_hit["highlight"]["phrases.texte"]
                    add_mydata["offset"] = phrase_hit["_nested"]["offset"]
                    mydata.append(add_mydata)

        if mydata:
            df = pd.DataFrame(mydata)
            df.sort_values(by=["patient_ipp", "keyword", "dateDebut"])

        else:
            df = pd.DataFrame(
                {
                    "id": [],
                    "patient_ipp": [],
                    "keyword": [],
                    "service": [],
                    "source": [],
                    "providerOrganisation": [],
                    "dateDebut": [],
                    "titre": [],
                    "type": [],
                    "sousType": [],
                    "sentence": [],
                    "sentence_order": [],
                    "sentence_highlighted": [],
                    "offset": [],
                }
            )

        return df

    def get_patient_inferee_json(
        self, patient_id, index_context="current", include=None, exclude=None
    ):
        """
        Méthode qui récupère le document json  d'un patient de l'index patient de Consore.

        Parameters
        ----------
        patient_id : string
            Identifiant du patient (ipp).
        index_context : string
            La recherche se fait sur les index consorepatient concaténés avec index_context.
            Par défaut on prend l index actuel.
        include: list
            Liste des champs du document à inclure.
        exclude: list
            Liste des champs à exclure du document.

        Returns
        -------
        total : dict | None
            Document du patient ou None si ce document n existe pas.

        """

        if not exclude:
            exclude = ["*.documents", "*.ecartsExamen"]

        if type(patient_id) == str:
            return_size = 1
            q_ipp = Q("term", ipp=patient_id)

        elif type(patient_id) == list:
            return_size = len(patient_id)
            q_ipp = Q("terms", ipp=patient_id)

        q_type = Q("term", _type="consorepatient")
        query = Q("bool", must=[q_ipp, q_type])

        response = Search(
            using=self.client, index="consorepatient" + index_context
        ).query(query)

        if exclude or include:
            response = response.source(include=include, exclude=exclude)

        total = response.params(size=return_size, request_timeout=50).execute()

        if total.hits.total == 1:
            return total.hits.hits[0]

        elif total.hits.total > 1:
            return total.hits.hits

        else:
            return None

    def get_document_json(
        self, doc_id, index_context="current", include=None, exclude=None
    ):
        """
        Méthode qui récupere un document de l index document de Consore.

        Parameters
        ----------
        doc_id : string | list
            Identifiant(s) du document.
        index_context : string
            La recherche se fait sur les index consoredocument concaténés avec index_context.
            Par défaut on prend l index actuel.
        include: list
            Liste des champs du document à inclure.
        exclude: list
            Liste des champs à exclure du document.

        Returns
        -------
        total : dict | None
            Document ou None si ce document n existe pas.

        """

        if type(doc_id) == str:
            return_size = 1
            q_doc_id = Q("term", id=doc_id)

        elif type(doc_id) == list:
            return_size = len(doc_id)
            q_doc_id = Q("terms", id=doc_id)

        q_type = Q("term", _type="consoredocument")
        query = Q("bool", must=[q_doc_id, q_type])

        response = Search(
            using=self.client, index="consoredocument" + index_context
        ).query(query)

        if exclude or include:
            response = response.source(include=include, exclude=exclude)

        total = response.params(size=return_size, request_timeout=50).execute()

        if total.hits.total == 1:
            return total.hits.hits[0]

        elif total.hits.total > 1:
            return total.hits.hits

        else:
            return None

    def get_patient_documents_json(
        self,
        patient_id,
        index_context="current",
        source_filter=None,
        include=None,
        exclude=None,
        batch_size=1000,
    ):
        """
        Méthode qui récupere par défaut tous les document CR et Fiche d un patient
        de l index document de Consore.

        Parameters
        ----------
        doc_id : string
            Identifiant du document.
        index_context : string
            La recherche se fait sur les index consoredocument concaténés avec index_context.
             Par défaut on prend l index actuel.
        source_filter : list
            Liste de source de document en format string
        exclude: list
            Liste des champs à exclure du document.
        batch_size : int
            Nombre de documents à requêter à chaque itération du scan.
            Attention un batch_size > 1000 peut générer des timeout error.

        Returns
        -------
        doc_list : list
            Liste de documents. Attention les documents peuvent être volumineux.

        """

        if not source_filter:
            source_filter = ["CR", "Fiche tumeur"]

        q_ipp = Q("term", ipp=patient_id)

        q_type = Q("term", _type="consoredocument")
        q_source = Q("terms", source=source_filter)

        query = Q("bool", must=[q_ipp, q_type, q_source])

        response = Search(
            using=self.client, index="consoredocument" + index_context
        ).query(query)

        if exclude or include:
            response = response.source(include=include, exclude=exclude)

        total = response.params(size="1", request_timeout=50).execute()
        total_hits = total.hits.total
        print("Nombre de documents total: ", total_hits)

        scan = response.params(scroll="10m", size=batch_size).scan()

        doc_list = []

        print("start collecting data...")
        for n, hit in enumerate(scan):
            doc_list.append(hit.to_dict())

        return doc_list

    def get_documents_json(
        self,
        patient_ids=None,
        index_context="current",
        source_filter=None,
        include=None,
        exclude=None,
        date_start_filter=None,
        date_end_filter=None,
        date_field="dateDebut",
        batch_size=1000,
    ):
        """
        Méthode qui récupere par défaut tous les document CR et Fiche
        d une list de patient de l index document de Consore.
        avec des parametres comme la source la date ou des listes de patient

        Parameters
        ----------
        doc_id : string
            Identifiant du document.
        index_context : string
            La recherche se fait sur les index consoredocument concaténés avec index_context.
             Par défaut on prend l index actuel.
        source_filter : list
            Liste de source de document en format string
        exclude: list
            Liste des champs à exclure du document.
        batch_size : int
            Nombre de documents à requêter à chaque itération du scan.
             Attention un batch_size > 1000 peut générer des timeout error.
        date_start_filter : date
            borne inferieur selection date
        date_end_filter : date
            borne superieur selection date
        date_field: string
            nom du champ date sur lequel appliquer la borne
        Returns
        -------
        doc_list : list
            Liste de documents. Attention les documents peuvent être volumineux.

        """

        if not source_filter:
            source_filter = ["CR", "Fiche tumeur"]

        q_ipp_list = None
        if patient_ids is not None:
            q_ipp_list = Q("terms", ipp=patient_ids, boost=1.0)
        q_type = Q("term", _type="consoredocument")

        q_source = None
        if source_filter is not None:
            q_source = Q("terms", source=source_filter)

        q_date = None
        if date_start_filter is not None and date_end_filter is not None:
            if date_field == "dateDebut":
                q_date = Q(
                    "range",
                    dateDebut={
                        "from": f"{date_start_filter}",
                        "to": f"{date_end_filter}",
                        "include_lower": True,
                        "include_upper": True,
                        "format": "yyyy-MM-dd",
                    },
                )
            elif date_field == "dateFin":
                q_date = Q(
                    "range",
                    dateFin={
                        "from": f"{date_start_filter}",
                        "to": f"{date_end_filter}",
                        "include_lower": True,
                        "include_upper": True,
                        "format": "yyyy-MM-dd",
                    },
                )
            else:
                raise Exception(f"{date_field}  not implemented as filter")

        must_query = [q_ipp_list, q_date, q_type, q_source]
        must_query = [x for x in must_query if x is not None]

        query = Q("bool", must=must_query)

        response = Search(
            using=self.client, index="consoredocument" + index_context
        ).query(query)
        response = response.source(excludes=exclude, includes=include)

        total = response.params(size="1", request_timeout=50).execute()
        total_hits = total.hits.total
        print("Nombre de documents total: ", total_hits)

        scan = response.params(scroll="10m", size=batch_size).scan()

        doc_list = []

        print("start collecting data...")
        for n, hit in enumerate(scan):
            meta_data = hit.meta.to_dict()
            doc_data = hit.to_dict()
            # Fusionnez les champs de meta_data dans doc_data.
            doc_data = {**meta_data, **doc_data}
            doc_list.append(doc_data)

        return doc_list

    def getpmsidata(
        self, date_deb=None, date_fin=None, patient_ids=None, date_field=None
    ):
        """
        Méthode qui récupere les données du pmsi depuis l'index document de consore
        les arguments de la fonction sont des filtres de selections des données pmsi

        Parameters

        patient_ids: List:
            liste de numéro dossier patients  sur lesquels restreindre la selection
        date_deb : date
            borne inferieur selection date
        date_fin : date
            borne superieur selection date
        date_field: string
            nom du champ date sur lequel appliquer la borne
        Returns
        -------
        mydf : Dataframe


        """
        lst = self.get_documents_json(
            patient_ids=patient_ids,
            index_context="current",
            source_filter=["PMSI"],
            include=None,
            exclude=[
                "texte",
                "hypotheses",
                "negations",
                "tumeurs",
                "phrases",
                "donneesGeneriques",
                "examenBiologie",
                "examenImagerie",
            ],
            date_start_filter=date_deb,
            date_end_filter=date_fin,
            date_field=date_field,
            batch_size=1000,
        )

        records = []

        for doc in lst:
            sejour_id = doc["id"].replace("PMSI_", "")
            ipp = doc["ipp"]
            date_entree = doc["dateDebut"][:10]
            date_sortie = doc["dateFin"][:10]
            actes = ""
            if "actes" in doc and doc["actes"] is not None:
                for acte in doc["actes"]:
                    cur_acte = acte["uri"].replace("ccam:", "")
                    if cur_acte not in actes:
                        actes = actes + "," + cur_acte
            diags = ""
            if "diagnostics" in doc and doc["diagnostics"] is not None:
                for diag in doc["diagnostics"]:
                    cur_diag = diag["uri"].replace("cim10:", "")
                    if len(cur_diag) >= 3:
                        cur_diag = cur_diag + "," + cur_diag[:3]
                    cur_dtype = diag["type"]
                    if cur_diag not in diags:
                        diags = diags + "," + cur_diag
            c_record = {
                "patient_ipp": ipp,
                "code_visite": sejour_id,
                "entreele": date_entree,
                "sortie": date_sortie,
                "diagcodes": diags,
                "actes": actes,
            }
            records.append(c_record)

        mydf = pd.DataFrame.from_records(records)

        return mydf
