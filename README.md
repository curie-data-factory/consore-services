
# Installation

```
conda create --name consore-services python=3.9.2
conda activate consore-services
pip install poetry==1.6.1
pip install pre-commit
```

```
cd PROJECTLOCATION
pre-commit install
poetry install --no-root
```

# Documentation

Le projet se compose de services et d'une toolbox d'interactions avec les indexes Consore. 

# Services disponibles
### 1.[Controle codage PMSI README](consore_services%2Fcontrole_codage_pmsi%2FREADME.md)

Le service propose de compléter le codage PMSI avec des recherches de mots clefs dans Consore afin d'identifier des codes diagnostiques manquant du codage PMSI.

# Toolbox
Cette toolbox est un ensemble de méthodes de requettage et d'extraction des données des indexes Consore.  
Elle est utilisée par les services mis à disposition dans ce repo, mais elle peut également servir à d'autres besoins internes d'extractions.



