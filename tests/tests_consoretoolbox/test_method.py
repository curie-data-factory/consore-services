import unittest
from unittest.mock import patch

import pandas as pd

from consoretoolbox.consore import Consore
from util.compare_util import compare_dataframe


class TestYourClass(unittest.TestCase):
    @patch("consoretoolbox.consore.Consore.get_documents_json")
    def test_getpmsidata_valid_parsing(self, mock_get_documents_json):
        # Simulez les données JSON de réponse que vous attendez
        mock_response = [
            {
                "id": "PMSI_1",
                "ipp": "123456",
                "dateDebut": "2023-01-01T00:00:00Z",
                "dateFin": "2023-01-10T00:00:00Z",
                "actes": [{"uri": "ccam:123"}, {"uri": "ccam:124"}],
                "diagnostics": [
                    {"uri": "cim10:456", "type": "principal"},
                    {"uri": "cim10:456", "type": "associe"},
                ],
            },
            {
                "id": "PMSI_2",
                "ipp": "123456",
                "dateDebut": "2023-01-01T00:00:00Z",
                "dateFin": "2023-01-10T00:00:00Z",
                "actes": [{"uri": "ccam:234"}, {"uri": "ccam:235"}],
                "diagnostics": [
                    {"uri": "cim10:789", "type": "principal"},
                    {"uri": "cim10:567", "type": "associe"},
                ],
            },
        ]

        ref_dict = {
            "patient_ipp": {0: "123456", 1: "123456"},
            "code_visite": {0: "1", 1: "2"},
            "entreele": {0: "2023-01-01", 1: "2023-01-01"},
            "sortie": {0: "2023-01-10", 1: "2023-01-10"},
            "diagcodes": {0: ",456,456", 1: ",789,789,567,567"},
            "actes": {0: ",123,124", 1: ",234,235"},
        }
        ref_df = pd.DataFrame.from_dict(ref_dict)

        fake_creds = {}
        cons = Consore(fake_creds)
        # Configurez le comportement simulé de get_documents_json
        mock_get_documents_json.return_value = mock_response
        date_deb = "2023-01-01"
        date_fin = "2023-01-10"
        result = cons.getpmsidata(date_deb, date_fin)
        print(result.to_dict())
        # Assurez-vous que le résultat est une instance de pandas DataFrame

        self.assertIsInstance(result, pd.DataFrame)
        # Assurez-vous que le DataFrame contient des données, c'est-à-dire qu'il n'est pas vide
        self.assertFalse(result.empty)
        compare_dataframe(result, ref_df)
