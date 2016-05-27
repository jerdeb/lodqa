# LOD Cloud Quality Assessment

This repository contains a number of scripts that enables the quality assessment of the LOD Cloud.

There are two modules in the GIT repository

## LODObserver Module

This module contains scripts that crawls the LOD Cloud snapshot and create an observer metadata:
```
<http://purl.org/obs/resource#2001-spanish-census-to-rdf> a ns1:Dataset ;
    ns2:modified "2015-12-18T13:23:22.256298"^^xsd:dateTime ;
    ns2:source <http://datahub.io/dataset/2001-spanish-census-to-rdf> ;
    ns1:category "government"^^xsd:string ;
    ns1:namespace <> ;
    ns1:payLevelDomain <http://dataweb.infor.uva.es/census2001> ;
    ns3:dataDump <http://visualdataweb.infor.uva.es/censo/RDFData.html>,
        <http://visualdataweb.infor.uva.es/censo/census90M.n3.gz>,
        <http://visualdataweb.infor.uva.es/census/resource/edificios>,
        <http://visualdataweb.infor.uva.es/census/resource/hogares>,
        <http://visualdataweb.infor.uva.es/census/resource/nucleos>,
        <http://visualdataweb.infor.uva.es/census/resource/personas> ;
    ns3:sparqlEndpoint <http://visualdataweb.infor.uva.es/sparql> .
```
In this module there are three scripts:

1) lodobserver.py - crawls the snapshot and create the metadata;
2) lodobserver_withCategory.py - same as lodobserver but adds categories (assigned from the LOD cloud) to the metadata;
3) lodExperiments.py - create statistics out of the observed data.

## LODQA Module

This module deals with the quality assessment. The Luzzu Quality Assessment framework (https://github.com/eis-bonn/Luzzu/) is required to be installed and running beforehand.

In this module there the following files/scripts

1) main.py - the main script for running the quality assessment. For this, the quality metrics have to be defined in config.ttl;
2) generateCategoriesForLuzzu.py - this script generates a file with categories for each dataset, which then should be used for the assessment of the Reuse Existing Terms metric;
3) preprocess.sh - downloads the datasets' data dumps and pre-process them prior to assessment (if a dataset's dump is already downloaded, it is not redownloaded).


## Scripts folder

In the scripts folder, there are a number of installation scripts (for ubuntu) that are required to run these experiments.
We suggest that such installation and experiments are performed on a virtual machine or docker instances.

Steps:

1. `$ sudo chmod +x preInstall.sh`
2. `$ sudo chmod +x luzzu.sh`
3. `$ sudo ./preInstall.sh`
4. `$ sudo ./luzzu.sh`

Once everything is installed, run Luzzu as per the instructions in (https://github.com/eis-bonn/Luzzu/).

## License
This work is licensed under the MIT licensed

## How to Cite
```
@article{debattistalod,
  title={Are LOD Datasets Well Represented? A Data Representation Quality Survey.},
  author={Debattista, Jeremy and Lange, Christoph and Auer, S{\"o}ren},
  url={https://www.researchgate.net/publication/301765676_Are_LOD_Datasets_Well_Represented_A_Data_Representation_Quality_Survey}
}
```

## Publications
Are LOD Cloud Datasets Well Represented? A Data Representation Quality Survey (Under Review) - [pdf](https://www.researchgate.net/publication/301765676_Are_LOD_Datasets_Well_Represented_A_Data_Representation_Quality_Survey)

## Acknowledgements
I would like to thank Sören Auer, Christoph Lange, and Aidan Hogan for their valuable contribution towards this work.
