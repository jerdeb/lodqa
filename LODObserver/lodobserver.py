# The idea of this experiment is to show that there is some way of accessing a resource, 
# but further experiments are done during quality assessment

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


# adding namespaces
dcat = Namespace("http://www.w3.org/ns/dcat#")
res = Namespace("http://purl.org/obs/resource#")
obs = Namespace("http://purl.org/obs#")

# Logging Configuration
logging.basicConfig(filename='logger.log', level=logging.DEBUG, format="%(message)s")
logger  = logging.getLogger("observer")
logger.setLevel(logging.DEBUG)


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

         
# Constants
SPARQL_FORMAT_TAG = [
    'api/sparql'
]

DUMP_FORMAT_TAG = [
    'application/rdf+xml',
    'text/turtle',
    'application/x-ntriples',
    'application/x-nquads',
    'text/n3',
    'rdf',
    'text/rdf+n3',
    'rdf/turtle'
]

graph_dump = Graph()

# Functions
def captureCKANMetadata(pld,ns, uri, uid):
    
    observedID = URIRef(res + uid)
    graph_dump.add((observedID, RDF.type, obs.Dataset))
    
    sourceURI = URIRef(uri)
    graph_dump.add((observedID, DCTERMS.source, sourceURI))
    
    currentDate = datetime.datetime.now()
    modified = Literal(currentDate, datatype=XSD.dateTime)
    graph_dump.add((observedID, DCTERMS.modified, modified))
    
    namespace = URIRef(ns);
    graph_dump.add((observedID, obs.namespace, namespace));
    
    if (pld):
        pld = URIRef(pld);
        graph_dump.add((observedID, obs.payLevelDomain, pld));
    
    
    
    print uri
    logger.debug("Datahub URL: {0}; PLD: {1}; NS: {2}".format(uri, pld, ns))
    g = rdflib.Graph()
    try:
        g.parse(uri+'.rdf')
    except: #if it fails for some timeout, try again
        print 'Socket Timeout'
        time.sleep(60)
        g = rdflib.Graph()
        g.parse(uri+'.rdf')
    voidURI = ''
    sparql = ''
    datadump = ''
    dumpList = set()
    
    # get distributions
    for row in g.query("""SELECT *  WHERE
        {   ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> . 
            ?ds <http://purl.org/dc/terms/format> ?fm .
            ?fm <http://www.w3.org/2000/01/rdf-schema#label> ?format .
            ?ds <http://www.w3.org/ns/dcat#accessURL> ?url .
            OPTIONAL {?ds <http://purl.org/dc/terms/title> ?title .}
        }"""):
            distribFormat = "%s"%(row["format"])
            distribURL = "%s"%(row["url"])
            distribTitle = "%s"%(row["title"])
            
            if ('void' in distribFormat.lower() or 'void' in distribURL.lower() or 'void' in distribTitle.lower()):
                voidURI = distribURL
            
            if (distribFormat.lower() in DUMP_FORMAT_TAG):
                if ((not 'example' in distribTitle.lower()) and 
                    (not 'void' in distribTitle.lower()) and 
                    (not 'void' in distribURL.lower())):
                    datadump = distribURL #if distribURL contains void, then its not void
                    dumpList.add(distribURL)
            
            if (distribFormat.lower() in SPARQL_FORMAT_TAG):  
                sparql = distribURL
    

    validationOK = False
    validationColor = "#000000"
    stage = 0
    
    voidColor = "#FF0000" #red
    dumpColor = "#00FF00" #green
    endpointColor = "#0000FF" #blue

    allStages = "#FEDCBA" #peach
    
    if (voidURI):
        print 'voID: '+voidURI
        try: #Quick hack
            logger.debug("voID: {0}".format(voidURI)) 
        except:
            logger.debug("voID: cannot decode uri")
        ok = query_void(ns, voidURI)
        validationOK = ok
        
        if (validationOK):
            stage = 1
            validationColor = voidColor
            encodedURI = werkzeug.urls.url_fix(werkzeug.urls.url_fix(voidURI))
            voidURI = URIRef(encodedURI)
            graph_dump.add((observedID, obs.hasCorrespondingVoid, voidURI))
    else:
        if pld:
            tryURI = pld + '/.well-known/void'
            print 'well-known voID: '+ tryURI
            logger.debug("voID: {0}".format(tryURI)) 
            ok = query_void(ns, tryURI)
            validationOK = ok
            if (validationOK):
                stage = 1
                validationColor = voidColor
                encodedURI = werkzeug.urls.url_fix(werkzeug.urls.url_fix(tryURI))
                voidURI = URIRef(encodedURI)
                graph_dump.add((observedID, obs.hasCorrespondingVoid, voidURI))
                
    if (sparql):
        print 'endpoint: '+ sparql
        logger.debug("\n")
        logger.debug("SPARQL Endpoint: {0}".format(sparql)) 
        validationOK = query_endpoint(sparql)
        if (validationOK):
            stage = stage + 1
            validationColor = addition(validationColor,endpointColor)
            encodedURI = werkzeug.urls.url_fix(werkzeug.urls.url_fix(sparql))
            endpointURI = URIRef(encodedURI)
            graph_dump.add((observedID, VOID.sparqlEndpoint, endpointURI))
    else:
        if pld:
            tryEP = pld + '/sparql'
            logger.debug("\n")
            logger.debug("SPARQL Endpoint: {0}".format(tryEP)) 
            print 'trying endpoint: '+ tryEP
            validationOK = query_endpoint(tryEP)
            if (validationOK):
                stage = stage + 1
                validationColor = addition(validationColor,endpointColor)
                encodedURI = werkzeug.urls.url_fix(werkzeug.urls.url_fix(tryEP))
                endpointURI = URIRef(encodedURI)
                graph_dump.add((observedID, VOID.sparqlEndpoint, endpointURI))
                
    if (datadump):
        print 'datadump: '+datadump
        logger.debug("\n")
        try: #Quick hack
            logger.debug("Datadump: {0}".format(datadump)) 
        except:
            logger.debug("Datadump: cannot decode uri")
        validationOK = validate_dump(datadump)
        if (validationOK):
            stage = stage + 1
            validationColor = addition(validationColor,dumpColor)
            for dd in dumpList:
                encodedURI = werkzeug.urls.url_fix(werkzeug.urls.url_fix(dd))
                ddURI = URIRef(encodedURI)
                graph_dump.add((observedID, VOID.dataDump, ddURI))
    # else try to dereference 
    
    print 'Validation Stages: ' + str(stage)
    logger.debug("\n")
    logger.debug("Total Validation Stages: {0}; Color: {1}".format(str(stage), validationColor)) 
    logger.debug("==================")
    logger.debug("\n")
    if (stage == 3):
        validationColor = allStages
    
    print "=================="
    
    if (validationColor == '#000000'):
        return "#a4a4a4"
    else:
        return validationColor

def query_void(url, voidurl):
    if (tryFetch(voidurl)):
        try:
            graph = rdflib.Graph()
            graph.parse(voidurl)
            accessible = False
            
            result = graph.query('ASK { ?s a <http://rdfs.org/ns/void#Dataset> . }')
            for row in result:
                accessible = bool(row) 
         
            logger.debug("voID results: {0}".format(str(accessible)))
            return accessible
        except:
            e = sys.exc_info()[0]
            logger.debug("Exception: {0}".format(e))
            return False
    else:
        return False
        
        
def query_endpoint(uri):
    try:
        sparql = SPARQLWrapper(uri)
        sparql.setQuery('ASK {?s ?p ?o}')
        sparql.setReturnFormat(XML)
        sparql.setTimeout(3)
        results = sparql.query().convert()
        logger.debug("Endpoint results: {0}".format(results.toxml()))
        
        for result in results.getElementsByTagName('boolean'):
            return True
        return False 
    except (EndPointInternalError, AttributeError) as epex:
        logger.debug("Exception: {0}".format(epex))
        logger.debug("Trying without SPARQL Wrapper")
        try:
            params = urllib.urlencode({'query': 'ASK {?s ?p ?o}'})
            opener = urllib2.build_opener(urllib2.HTTPHandler)
            request = urllib2.Request(uri+'?'+params)
            request.get_method = lambda: 'GET'
            request.add_header('Accept', 'application/sparql-results+json')
            url = opener.open(request, timeout=3)
            data = url.read()
            results = json.loads(data)
            if(results['boolean'] is not None):
                return True
            else:
                return False
        except Exception as noContentNego:
            logger.debug("Exception: {0}".format(noContentNego))
            logger.debug("Trying without Content Negotiation")
            try:
                params = urllib.urlencode({'query': 'ASK {?s ?p ?o}'})
                opener = urllib2.build_opener(urllib2.HTTPHandler)
                request = urllib2.Request(uri+'?'+params)
                request.get_method = lambda: 'GET'
                url = opener.open(request, timeout=3)
                data = url.read()
                if (data):
                    return True
                else: 
                    return False
            except:
                e = sys.exc_info()[0]
                logger.debug("Exception: {0}".format(e))
                return False
    except:
        e = sys.exc_info()[0]
        logger.debug("Exception: {0}".format(e))
        return False
 
def validate_dump(dumpuri):
    try:
        resp = requests.head(dumpuri, timeout=2, allow_redirects=True)
        logger.debug("Dump Accessibility:  {0}".format(resp.status_code)) 
        if (resp.status_code == 200):
            return True
        else:
            return False
    except:
        e = sys.exc_info()[0]
        logger.debug("Exception: {0}".format(e))
        return False


# try fetch
def tryFetch(uri):
    try:
        resp = requests.head(uri, timeout=5)
        return True
    except:
        return False


# Coloring
_NUMERALS = '0123456789abcdefABCDEF'
_HEXDEC = {v: int(v, 16) for v in (x+y for x in _NUMERALS for y in _NUMERALS)}
LOWERCASE, UPPERCASE = 'x', 'X'

def rgb(triplet):
    return _HEXDEC[triplet[0:2]], _HEXDEC[triplet[2:4]], _HEXDEC[triplet[4:6]]

def triplet(rgb, lettercase=LOWERCASE):
    return format(rgb[0]<<16 | rgb[1]<<8 | rgb[2], '06'+lettercase)
    
def addition(first, second):
    if (first == second):
        return first
    first = first[1:]
    second = second[1:]
    triplet1 = rgb(first)
    triplet2 = rgb(second)

    r = min((triplet1[0] + triplet2[0]),255)
    g = min((triplet1[1] + triplet2[1]),255)
    b = min((triplet1[2] + triplet2[2]),255)

    newColor = triplet((r, g, b), UPPERCASE)
    return '#'+str(newColor)



# Main
tree = etree.parse(open('lod-cloud.svg','r'))
saveFile = 'lod-cloud-mod-test.svg'
ckanconnectionLDC = ckanapi.RemoteCKAN("http://linkeddatacatalog.dws.informatik.uni-mannheim.de",user_agent='ckanapiexample/1.0 (+http://jerdeb.github.io)')
ckanconnectionDH = ckanapi.RemoteCKAN("http://datahub.io",
apikey='5e35c0d1-19c0-4f93-8b9f-14a0d507990c',
get_only=True)

#testing
# ckan_record = ckanconnectionDH.action.package_show(id='abs-linked-data')
# validationColor = captureCKANMetadata('http://abs.270a.info/', 'http://abs.270a.info/dataset/','http://datahub.io/dataset/abs-linked-data','abs-linked-data')
# print validationColor



# extract pld
#http://stackoverflow.com/questions/10552188/python-split-url-to-find-image-name-and-extension

# extract tld
#http://stackoverflow.com/questions/14406300/python-urlparse-extract-domain-name-without-subdomain

for element in tree.iter():
    if element.tag.split("}")[1] == 'a':
        uri = element.get('{http://www.w3.org/1999/xlink}href')

        print 'id: '+ uri[26:]


        try:
          ckan_record = ckanconnectionDH.action.package_show(id=uri[26:])
        except:
            ckan_record = ckanconnectionLDC.action.package_show(id=uri[26:])
            uri = uri.replace('http://datahub.io/dataset/','http://linkeddatacatalog.dws.informatik.uni-mannheim.de/dataset/')

        pld = ckan_record['url']
        if (not pld):
            for extra in ckan_record['extras']:
              if extra['key'] == 'url':
                pld = extra['value']
        ns = ''
        for extra in ckan_record['extras']:
            if extra['key'] == 'namespace':
                ns = extra['value']

        if (not pld):
            print 'PLD: unknown'
        else:
            print 'PLD: '+pld
            pld = pld.strip()

        if (not ns):
            print 'NS: unknown'
        else:
            print 'NS: '+ns
            ns = ns.strip()

        validationColor = captureCKANMetadata(pld, ns, uri, uri[26:])

        # Checking the child group of the element
        for child_elem in element.iter():
            if child_elem.tag.split('}')[1] == 'g':
                if (child_elem.get('id') == 'Oval' and child_elem.get('fill') is not None) :
                    ## ok we found our oval
                    child_elem.set('fill',validationColor);
                    with open(saveFile,'w') as f:
                        f.write(etree.tostring(tree))
        graph_dump.serialize("observed.ttl", format="turtle")