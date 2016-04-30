import rdflib
from rdflib.plugin import register, Serializer, Parser
from rdflib.namespace import DCTERMS, VOID, RDF, XSD
from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef

graph_dump = Graph()
graph_dump.parse('observed_withCategories.ttl', format="turtle")

graph = Graph()


query = """SELECT * WHERE { 
    ?x a <http://purl.org/obs#Dataset> .
    ?x <http://purl.org/obs#category> ?cat .
    OPTIONAL { ?x <http://purl.org/obs#payLevelDomain> ?pld . }
    ?x <http://purl.org/dc/terms/source> ?source .
}"""

for row in graph_dump.query(query):
    category = Literal(row["cat"], datatype=XSD.string)
    
    if ("pld" in row):   
        pld = URIRef(row["pld"])
    else:
        pld = URIRef("urn:"+row["source"])

    graph.add((pld, URIRef(":hasDomain"), category))
    if ("%s"%(row["cat"]).strip() in "user-generated content"):
        graph.add((pld, URIRef(":getFromLOV"), Literal(False, datatype=XSD.boolean)))
    elif("%s"%(row["cat"]).strip() in "cross domain"):
        graph.add((pld, URIRef(":getFromLOV"), Literal(False, datatype=XSD.boolean)))
    else:
        graph.add((pld, URIRef(":getFromLOV"), Literal(True, datatype=XSD.boolean)))
    
graph.serialize("lod-categories-luzzu.ttl", format="turtle")