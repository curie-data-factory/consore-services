
# Compatibilité du service
* Consore version 2022.2 (alimenté avec la source PMSI et les documents)


# Fonctionnement du programme

Le service propose de compléter le codage PMSI avec des recherches de mots clefs dans Consore afin d'identifier des codes diagnostiques manquant du codage PMSI.


![ControlecodagePMSI.JPG](..%2F..%2Fimg%2FControlecodagePMSI.JPG)

Depuis Consore on extrait
- les sejours et diagnostics
  - les séjours sont extraits entre deux dates fournies en entrée du service
- les phrases correspondant à des mots clefs
  - les mots clefs sont fournis en entrée du service dans un fichier excel qui contient un mapping entre mots clefs et codes CIM10


On match les codes diagnostics issus du texte et ceux du PMSI par date (la date du document doit être comprise entre les dates du séjour).

Puis on filtre les phrases dont le code est déja présent dans le séjour afin de ne garder que les codes manquant.

la sortie est un tableau excel contenant les séjours, les phrases et les codes CIM10 manquant du codage initial.

# Filtre séjours
Sont filtrés 
- les séjours de moins de 2 jours, la valorisation des séjours de moins de 2 jours est la même quelque soit le codage des comorbidités/complications.
- les séjours de radiothérapie, la valorisation des séjours de radiothérapie est liée uniquement à la technique de radiothérapie utilisée.



# Fichiers d'entrée 

Le fichier de keywords est un fichier excel xlsx et doit contenir deux colonnes

- keyword [mot clef comme dans Consore]
- codes [codes diagnostics séparés par des virgules]

### Exemple de fichier keyword:

![CCP_keywords_input_example.JPG](..%2F..%2Fimg%2FCCP_keywords_input_example.JPG)


# Fichiers de sortie 


Un seul fichier de sortie va être crée par le programme.
Il correspond aux diagnostics trouvés par la recherche texte non présents dans les séjours extraits du PMSI. 
Il sera déposé dans le meme répertoire que le fichier de keywords fourni en entrée.
```
exemple :  
  input :  
    C:/controle_codage/run1/keywords.xlsx  
  output:   
    C:/controle_codage/run1/keywords.xlsx
```



# Modalité de connexion à Consore

Editer le fichier creds/consore.json 

```
{
  "host": "consore.curie.net",
  "password": "*********",
  "port": "9201",
  "user": "user"
}
```

# Modalité d'exécution

Installer l'environnement anaconda voir [README.md](..%2F..%2FREADME.md)
 
Exécuter le programme  src/controle_codage_pmsi/main.py<br />
avec les arguments suivants :<br />
- consore  [Nom du fichier credential d'accès a consore dans le dossier creds  (consore.json) ]
- inputkeywords [ Chemin absolu du fichier de keywords d'entrée]
- datedeb [ Date de début de sélection des séjours]
- datefin [ Date de fin de sélection des séjours]

Exemple de ligne de commande :
> python consore_services/controle_codage_pmsi/main.py --consore consore.json   --inputkeywords C://Users/tbalezea/PycharmProjects/controle-codage-pmsi/tests/output/keywords.txt --datedeb 2021-01-01  --datefin 2021-01-09



# Exécution des tests

Lancer les tests du services avec la commande suivante 
> pytest -vvv tests/tests_controle_codage

La fonction test_compare_directory correspond à un test end to end du programme.

Il prend en entrée le fichier
- tests/tests_controle_codage/output/keywords.xlsx  

pour produire la sortie 
- tests/tests_controle_codage/output/keywords_output.xlsx

Les données de séjours et de phrases sont mockées et présentes dans les fichiers suivants
- tests/tests_controle_codage/referentiels/mock_data/sejours.csv
- tests/tests_controle_codage/referentiels/mock_data/sentences.csv


# Plugin Curie

Consore ne permet pas d'injecter les codes GHM dans la source PMSI.  
Nous avons donc rajouté un plugin qui permet en une requête sur une base mysql propre à Curie de récuperer les codes GHM, les libellés GHM pour chaque séjour.   
Ces données sont ensuite concaténées aux données du séjour et ajoutées au fichier de sortie.   
Pour exécuter ce plugin il faut renseigner dans la ligne de commande : 

 > --plugin_db [nom du fichier credential d'accès a la base dans le dossier creds]
 
La fonction de récupération des GHM est dans le fichier plugin_curie_pmsi.py.   
La requête et le requettage est à adapter en fonction de chaque centre.  