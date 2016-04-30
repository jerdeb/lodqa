import rdflib
import subprocess
import requests
import unirest
import logging
import sys
import json
import threading
import os
import time
from rdflib.plugin import register, Serializer, Parser

# Logging configuration
#logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(relativeCreated)d - %(name)s - %(levelname)s - %(message)s")
#logger_crawl = logging.getLogger("crawler")
#logger_crawl.setLevel(logging.DEBUG)


# Registering Parsers
register('text/rdf+n3', Parser, 'rdflib.plugins.parsers.notation3', 'N3Parser')
register('n3', Parser, 'rdflib.plugins.parsers.notation3', 'N3Parser')
register('nquads', Parser, 'rdflib.plugins.parsers.nquads', 'NQuadsParser')
register('nt', Parser, 'rdflib.plugins.parsers.nt', 'NTParser')
register('trix', Parser,'rdflib.plugins.parsers.trix', 'TriXParser')
register('application/rdf+xml', Parser, 'rdflib.plugins.parsers.rdfxml', 'RDFXMLParser')
register('xml', Parser, 'rdflib.plugins.parsers.rdfxml', 'RDFXMLParser')
register('rdfa', Parser, 'rdflib.plugins.parsers.rdfa', 'RDFaParser')
register('text/html', Parser, 'rdflib.plugins.parsers.rdfa', 'RDFaParser')
register('application/xhtml+xml', Parser,'rdflib.plugins.parsers.rdfa', 'RDFaParser')
register('text/plain', Parser, 'rdflib.plugins.parsers.notation3', 'N3Parser')         
register('application/octet-stream', Parser, 'rdflib.plugins.parsers.notation3', 'N3Parser')

# Variables
#birkirkara = '172.17.0.2:8080/Luzzu/compute_quality'
#sliema = '172.17.0.3:8080/Luzzu/compute_quality'
server = 'http://localhost:8080/Luzzu/compute_quality'
resources = set([])
#serverCount = dict({birkirkara:0, sliema:0})
#datasetsLocation = '/tmp/'
datasetsLocation = '/srv/datasets/'
setLock = threading.Lock()


# Object
class voidObject():
    uri = None
    datadump = set()
    sparql = None

# Functions        
def identifySerialisation(fileName):
    #add all serialisations
    if fileName.endswith(".ttl"):
        return "turtle"
    else:
        return
        
def loadMetricConfiguration():    
    g = rdflib.Graph();
    config = g.parse("config.ttl", format="turtle")
    return g.serialize(format="json-ld", indent=0)
    
def formatMetricConfiguration(configStr):
    formattedStr = configStr.replace('\n', ' ').replace('\r', '').replace('"','\"')
    return formattedStr


def check_status():
    toRemove = set([])
    print "== CHECK STATUS =="
    for r in resources:
        print "RequestID: "+ r
        payload = {'RequestID' : r};
        response = unirest.post("http://localhost:8080/Luzzu/status", headers={ "Accept": "application/json" }, params=payload)
        if (('Outcome' in response.body) or ('ErrorMessage' in response.body) or (response.body['Status'] == "Cancelled")):
            toRemove.add(r)
        print response.body
    for r in toRemove:
        resources.remove(r)

def call_luzzu(pld, uri, isSparql,server):        
  metricsConf = formatMetricConfiguration(loadMetricConfiguration())
  if (not isSparql):
      dataset = datasetsLocation+uri+".nt.gz"
  else:
      dataset = uri
  
  payload = {'Dataset' : dataset, 'QualityReportRequired' : 'true', 'MetricsConfiguration' : metricsConf, 'BaseUri' : pld, 'IsSparql' : isSparql }   
  
  print("Using Server: " + server + " for dataset: "+ dataset)
  response = unirest.post(server, headers={ "Accept": "application/json" }, params=payload)
  uuid = response.body["RequestID"];
  resources.add(uuid)
  
def assess_datadump(payLevelDomain,datadump,uri,isSparql,server):
    #p = subprocess.call(['./preprocess.sh', datadump, 'false', uri])
    call_luzzu(payLevelDomain, uri, False, server)
    
def assess_endpoint(payLevelDomain,sparqlEndpoint,server):
    call_luzzu(payLevelDomain, sparqlEndpoint, True,server)
    
def assess_void(payLevelDomain,void,server):
    gv = rdflib.Graph()
    gv.parse(void, format=identifySerialisation(void))
    voidObj = voidObject()
    
    for _voidRow in gv.query("""SELECT * { OPTIONAL {?x <http://rdfs.org/ns/void#dataDump> ?dd . } OPTIONAL {?x <http://rdfs.org/ns/void#sparqlEndpoint> ?spep . } }"""):
        voidObj.uri = _voidRow["x"]
        if (not _voidRow["dd"] is None):
            voidObj.datadump.add(_voidRow["dd"])
        voidObj.sparql = _voidRow["spep"]
        
    print voidObj.uri
    print voidObj.datadump 
    print voidObj.sparql
    
    if (len(voidObj.datadump) > 0):
        isSparql = False;
        uri = payLevelDomain.replace("http://", "")
        if (uri.endswith('/')):
            uri = uri[:-1]

        uri = uri.replace("/","_")
        voidObj.datadump.add(void)
        
        #p = subprocess.call(['./preprocess.sh', ','.join(voidObj.datadump), 'false', uri]);
        call_luzzu(payLevelDomain,uri, False,server);
    elif (not voidObj.sparql is None):
        call_luzzu(payLevelDomain,voidObj.sparql, True,server)

    
# Main
with open('observed.jsonld') as data_file:
    data = json.load(data_file)
    for row in data["@graph"]:
        sparqlEndpoint = None
        if ("ns3:sparqlEndpoint"  in row):
            sparqlEndpoint = (row["ns3:sparqlEndpoint"]["@id"])
        
        void = None
        if ("ns1:hasCorrespondingVoid"  in row):
            void = row["ns1:hasCorrespondingVoid"]["@id"]
            
        payLevelDomain = None
        if ("ns1:payLevelDomain"  in row):   
            payLevelDomain = (row["ns1:payLevelDomain"]["@id"])
            
        namespace = None
        if ("ns1:namespace" in row):   
            namespace = (row["ns1:namespace"]["@id"])
            
        if (payLevelDomain is None):
            payLevelDomain = row["ns2:source"]["@id"].replace("http://datahub.io/dataset/","urn:")
        
        datadump = None
        if ("ns3:dataDump" in row): 
            if isinstance(row["ns3:dataDump"], list):
                for dd in row["ns3:dataDump"]:
                    if (datadump is None):
                        datadump = ""
                    datadump = datadump + dd["@id"] + ","
            else:
                datadump = row["ns3:dataDump"]["@id"]
                
        # testPLD = payLevelDomain.replace("http://", "")
        # if (os.path.exists(qualityMetadataLocation+testPLD)):
        #     continue
                
        if (datadump is not None and datadump.endswith(',')):
            datadump = datadump[:-1]
                    
        if ((datadump is None) and (sparqlEndpoint is None) and (void is None)):
            continue
        else:        
            print ("Assessing: "+ payLevelDomain)
            time.sleep(5)
            i = 0
            while(len(resources) >= 3):
                check_status();
                time.sleep(10);
                
            uri = payLevelDomain.replace("http://", "")
            if (uri.endswith('/')):
                uri = uri[:-1]
            
            uri = uri.replace("/","_")
            
            
            if (os.path.isfile(datasetsLocation+uri+".nt.gz")):
                theSize = os.path.getsize(datasetsLocation+uri+".nt.gz");
                if (theSize < 100):
                    datadump = None
                    
            if (not datadump is None):
                if (not(os.path.isfile(datasetsLocation+uri+".nt.gz"))):
                    continue;
                    
                isSparql = False
                thr = threading.Thread(target=assess_datadump, args=(payLevelDomain,datadump,uri,isSparql,server,)).start()
            elif (not sparqlEndpoint is None):
                thr = threading.Thread(target=assess_endpoint, args=(payLevelDomain,sparqlEndpoint,server,)).start()
            elif (not void is None):
                thr = threading.Thread(target=assess_void, args=(payLevelDomain,void,server,)).start()
            time.sleep(10)
    
    time.sleep(10)         
    while(len(resources) > 0):
        check_status()
        time.sleep(10)
        
print "Assessment Ready"