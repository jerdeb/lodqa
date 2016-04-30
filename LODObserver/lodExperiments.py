import rdflib
import requests
import ckanapi
import json
import urllib,urllib2
import logging
import sys
import xml.dom
import datetime
import werkzeug
import time
from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef
from rdflib.namespace import DCTERMS, VOID, RDF, XSD
from lxml import etree
from SPARQLWrapper import SPARQLWrapper, JSON, XML
from SPARQLWrapper.SPARQLExceptions import EndPointInternalError
from rdflib.plugin import register, Serializer, Parser
import csv
import re

logging.basicConfig(filename='logger.log', level=logging.DEBUG, format="%(message)s")
logger  = logging.getLogger("observer")
logger.setLevel(logging.DEBUG)


# Variables
rights_dict=dict({}); # holds the number of times some license/right was used
ds_rights_pair = [] # a list with a dataset,right/licence pair
ds_possible_hr_rights_list = [] # a list with possible human readable license

# Set up a connection to the Linked Data Catalog where there are some of the crawled datasets available in LODCloud2014 but not LODCloud2011
ckanconnectionLDC = ckanapi.RemoteCKAN("http://linkeddatacatalog.dws.informatik.uni-mannheim.de",
                                     user_agent='ckanapiexample/1.0 (+http://jerdeb.github.io)')
                                     
# Set up a connection to the data hub containing the LODCloud2011 datasets
ckanconnectionDH = ckanapi.RemoteCKAN("http://datahub.io",
                        apikey='5e35c0d1-19c0-4f93-8b9f-14a0d507990c',
                        get_only=True)
                        
# Initialisation of LOD Snapshot
tree = etree.parse(open('lod-cloud-accessibility.svg','r')) # snapshot SVG

# Snapshot details
latestSnapshotDate = "30/08/2014";
downloadLink = "http://lod-cloud.net/versions/2014-08-30/lod-cloud.svg"

# Functions
def loadCKANMetadata(uri,_id):
    try:
        ckan_record = ckanconnectionDH.action.package_show(id=_id)
    except:
        ckan_record = ckanconnectionLDC.action.package_show(id=_id)
        uri = uri.replace('http://datahub.io/dataset/','http://linkeddatacatalog.dws.informatik.uni-mannheim.de/dataset/')
    
    g = rdflib.Graph()
    try:
        g.parse(uri+'.rdf')
    except: #if it fails for some timeout, try again
        time.sleep(60)
        g = rdflib.Graph()
        g.parse(uri+'.rdf')
    
    return g

def totalNumberOfDatasetsInSnapshot():
    count = 0;
    for element in tree.iter():
        if element.tag.split("}")[1] == 'a':
            count = count + 1
    return count
    
# def number of ds with access points
def totalNumberOfDatasetsWithAccessPoint():
    totalCount = 0;
    noAccessPoint = 0;
    for element in tree.iter():
        if element.tag.split("}")[1] == 'a':
            totalCount = totalCount + 1;
            for child_elem in element.iter():
                if child_elem.tag.split('}')[1] == 'g':
                    if (child_elem.get('id') == 'Oval'):
                        theColor = child_elem.get('fill');
                        if (theColor == "#FFFFFF"):
                            noAccessPoint = noAccessPoint + 1;
    
    print "Total Number of Datasets in LOD Cloud: "+ str(totalCount);
    print "Total Number of Accessible Datasets: " +str(totalCount - noAccessPoint);
    print "Total Number of Non-Accessible Datasets: " + str(noAccessPoint);
    
    
# def number of ds with just void
def datasetAccessStats():
    totalCount = 0;
    
    justVoid = 0;
    justDump = 0;
    justSparql = 0;
    
    sparqlDump = 0;
    sparqlVoid = 0;
    dumpVoid = 0;
    
    allthree = 0;
    
    for element in tree.iter():
        if element.tag.split("}")[1] == 'a':
            totalCount = totalCount + 1;
            for child_elem in element.iter():
                if child_elem.tag.split('}')[1] == 'g':
                    if (child_elem.get('id') == 'Oval'):
                        theColor = child_elem.get('fill');
                        if (theColor == "#FF0000"):
                            justVoid = justVoid + 1;
                        if (theColor == "#00FF00"):
                            justDump = justDump + 1;
                        if (theColor == "#0000FF"):
                            justSparql = justSparql + 1;
                            
                        if (theColor == "#00FFFF"):
                            sparqlDump = sparqlDump + 1;
                        if (theColor == "#FFFF00"):
                            sparqlVoid = sparqlVoid + 1;
                        if (theColor == "#FF00FF"):
                            dumpVoid = dumpVoid + 1;

                        if (theColor == "#FEDCBA"):
                            allthree = allthree + 1;
                                                        
    print "Total Number of Datasets in LOD Cloud: "+ str(totalCount);
    print "Total Number of voID Access Datasets: " +str(justVoid);
    print "Total Number of Dump Access Datasets: " +str(justDump);
    print "Total Number of SPARQL Access Datasets: " +str(justSparql);
    print "Total Number of Dump + SPARQL Access Datasets: " +str(sparqlDump);
    print "Total Number of voID + SPARQL Access Datasets: " +str(sparqlVoid);
    print "Total Number of voID + Dump Access Datasets: " +str(dumpVoid);
    print "Total Number of All Access Datasets: " +str(allthree);
    
# def number of ds with just sparql
# def number of ds with just dd

def totalNumberOfDatasetsWithLicenceOrRights():
    count = 0;
    for element in tree.iter():
        if element.tag.split("}")[1] == 'a':
            uri = element.get('{http://www.w3.org/1999/xlink}href')
            g = loadCKANMetadata(uri,uri[26:])

            for row in g.query("""SELECT *  WHERE
                {   ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> . 
                    OPTIONAL {?ds <http://purl.org/dc/terms/license> ?license . }
                    OPTIONAL { ?ds <http://purl.org/dc/terms/rights> ?rights .}
                }"""):
                    distribRights = row["rights"]
                    distribLicense = row["license"]
            
                    if ((not distribRights is None) or (not distribLicense is None)):
                        count = count + 1;                   
    return count
            
            
def colorLODCloudWithLicenseOrRights():
    saveFile = 'lod-cloud-licenses-rights.svg'
    rights = 0;
    possibleHR = 0;
    
    colRights = "#FF6347"

    for element in tree.iter():
        if element.tag.split("}")[1] == 'a':
            uri = element.get('{http://www.w3.org/1999/xlink}href')
            g = loadCKANMetadata(uri,uri[26:])

            toColor = "#FFFFFF"

            for row in g.query("""SELECT *  WHERE
                {   ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> . 
                    OPTIONAL {?ds <http://purl.org/dc/terms/license> ?license . }
                    OPTIONAL { ?ds <http://purl.org/dc/terms/rights> ?rights .}
                }"""):
                    distribRights = row["rights"]
                    distribLicense = row["license"]
                    
                    if (not distribRights is None):
                        toColor = colRights;
                        rights = rights + 1;
                        ds_rights_pair.append((row["ds"],distribRights))

                        if (not rights_dict.has_key(distribRights)):
                            rights_dict[distribRights] = 1
                        else:
                            currCount = rights_dict[distribRights];
                            currCount = currCount + 1;
                            rights_dict[distribRights] = currCount
                    else:
                        toColor = possibleHumanReadableLicenseExtraction(g)
                        
                        
                    if (toColor == "#FFFF00"):
                        possibleHR = possibleHR + 1
                       
            for child_elem in element.iter():
                if child_elem.tag.split('}')[1] == 'g':
                    if (child_elem.get('id') == 'Oval' and child_elem.get('fill') is not None) :
                        ## ok we found our oval
                        child_elem.set('fill',toColor);
                        with open(saveFile,'w') as f:
                            f.write(etree.tostring(tree))
            
    print "Number of datasets with rights only defined: "+str(rights);
    print "Number of datasets with a possible human readable licence definition: "+str(possibleHR);



def possibleHumanReadableLicenseExtraction(g):
    
    colPossible = "#FFFF00"
    toColor = "#FFFFFF"
            
    if (not uri in uris):
        for row in g.query("""SELECT *  WHERE
            {   ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> . 
                ?ds <http://purl.org/dc/terms/description> ?description . 
            }"""):
            description = tryDecoding("%s"%(row["description"]))  
                        
            str_list = description.splitlines()
            str_list = filter(None, str_list)
            new_desc = ' '.join([tryDecoding(x) for x in str_list])
    
            p = re.compile(r'.*(licensed?|copyrighte?d?).*(under|grante?d?|rights?).*',re.IGNORECASE | re.MULTILINE)
            m = p.match(new_desc)
                        
            if m:
                toColor = colPossible
                ds_possible_hr_rights_list.append(row["ds"])
                
    return toColor;            

def tryDecoding(text):
    try:
        text = unicode(text, 'utf-8')
        return text
    except TypeError:
        return text

    
def rights2CSV():
    print "Writing rights statistics to CSV file"
    with open('rights_stats.csv', 'wb') as csvfile:
        csvWriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for key,value in rights_dict.iteritems():
            csvWriter.writerow([key,str(value)])
            
    print "Writing rights usage of datasets to CSV file"
    with open('rights_dataset.csv', 'wb') as csvfile:
        csvWriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for key,value in ds_rights_pair:
            csvWriter.writerow([key,value])
    
    print "Writing possible human readable rights usage of datasets to CSV file"
    with open('possible_human_readable_rights.csv', 'wb') as csvfile:
        csvWriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for key,value in ds_possible_hr_rights_pair:
            csvWriter.writerow([key,value])
    

def recolor():
    tree = etree.parse(open('lod-cloud-mod.svg','r')) # snapshot SVG
    saveFile = 'lod-cloud-mod2.svg'
    
    for element in tree.iter():
        if element.tag.split("}")[1] == 'a':
            for child_elem in element.iter():
                if child_elem.tag.split('}')[1] == 'g':
                    if (child_elem.get('id') == 'Oval' and child_elem.get('fill') is not None) :                    
                        col = child_elem.get('fill');

                        if (col == "#a4a4a4"):
                            child_elem.set('fill',"#FFFFFF");
                    if (child_elem.get('id') == 'Oval' and child_elem.get('stroke') is not None) :
                        child_elem.set('stroke',"#000000");
    
    with open(saveFile,'w') as f:
        f.write(etree.tostring(tree))

# Calculates uptime difference withing the last 24hr and last week
def sparqlesAvailabilityDifference():
    totalChange = 0;
    totalEndpoints = 0;
    
    uptimeIncrease = 0;
    uptimeDecrease = 0;
    
    data = json.load(urllib2.urlopen('http://sparqles.ai.wu.ac.at/api/availability'))
    for element in data:
        totalEndpoints = totalEndpoints + 1
        twentyFour = element['uptimeLast24h']
        week = element['uptimeLast7d']
        
        change = twentyFour - week
        totalChange = totalChange + change;
        
        if (change != 0):
            print element['endpoint']['uri'] + " - " + str(change)
        
        if (change < 0):
            uptimeDecrease = uptimeDecrease + 1
            
        if (change > 0):
            uptimeIncrease = uptimeIncrease + 1
            
    print "Total Change : " + str(totalChange)
    print "Total Endpoints : " + str(totalEndpoints)
    print "Average Change : " + str(totalChange/totalEndpoints)
    print "Total Endpoints with Uptime Increase : " + str(uptimeIncrease)
    print "Total Endpoints with Uptime Decrease : " + str(uptimeDecrease)
    
# Calculates the number of endpoints that end with /sparql in their path
def sparqlInPath():
    totalEndpoints = 0;
    totalEndWithSPARQL = 0;
    
    data = json.load(urllib2.urlopen('http://sparqles.ai.wu.ac.at/api/availability'))
    for element in data:
        totalEndpoints = totalEndpoints + 1
        path = element['endpoint']['uri'];
        if (path.endswith('/sparql')): 
            totalEndWithSPARQL = totalEndWithSPARQL + 1;
        
    print "Total Endpoints : " + str(totalEndpoints)
    print "Total Paths Ending with /sparql: " + str(totalEndWithSPARQL)
    print "Ratio: " + str(totalEndWithSPARQL/totalEndpoints)
    
def countTriplesInEndPoints():
    totalSparqlEndPoints = 0;
    totalTriples = 0
    
    with open('observed_final.jsonld') as data_file:
        data = json.load(data_file)
        for row in data["@graph"]:
            if ("ns3:sparqlEndpoint" in row) and ((not ("ns3:dataDump" in row)) and (not ("ns1:hasCorrespondingVoid" in row))):
                cnt = query_endpoint(row["ns3:sparqlEndpoint"]["@id"]);
                print str(row["ns3:sparqlEndpoint"]["@id"]) + "," + str(cnt);
                totalTriples = totalTriples + int(cnt);
    
    print "total triples,"+str(totalTriples)
                
def query_endpoint(uri):
    try:
        params = urllib.urlencode({'query': 'SELECT DISTINCT (COUNT(?s) AS ?cnt) { ?s ?p ?o . }'})
        opener = urllib2.build_opener(urllib2.HTTPHandler)
        request = urllib2.Request(uri+'?'+params)
        request.get_method = lambda: 'GET'
        request.add_header('Accept', 'application/sparql-results+json')
        url = opener.open(request, timeout=3)
        data = url.read()
        results = json.loads(data)
        if(results['results']['bindings'][0]['cnt'] is not None):
            return results['results']['bindings'][0]['cnt']['value']
        else:
            return 0
    except (EndPointInternalError, AttributeError) as epex:
        logger.debug("Exception: {0}".format(epex))
        logger.debug("Trying without SPARQL Wrapper")
        try:
            sparql = SPARQLWrapper(uri)
            sparql.setQuery('SELECT DISTINCT (COUNT(?s) AS ?cnt) { ?s ?p ?o . }');
            sparql.setReturnFormat(XML)
            sparql.setTimeout(3)
            results = sparql.query().convert()
            logger.debug("Endpoint results: {0}".format(results.toxml()))
        
            for result in results.getElementsByTagName('literal'):
                return result[0].text;
            return 0
        except Exception as noContentNego:
            return 0;
    except:
        e = sys.exc_info()[0]
        logger.debug("Exception: {0}".format(e))
        return 0

# Main
# print "Total number of datasets in snapshot: " + str(totalNumberOfDatasetsInSnapshot())
# print "Coloring LOD Snapshot with Licenses and Rights"
# colorLODCloudWithLicenseOrRights()
# rights2CSV()
# sparqlesAvailabilityDifference()
#sparqlInPath()
#totalNumberOfDatasetsWithAccessPoint()
#datasetAccessStats()
countTriplesInEndPoints();