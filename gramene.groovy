import groovy.json.JsonSlurper

@Grab('org.neo4j:neo4j:2.1.4')

// I installed neo4j using homebrew
String store = "/usr/local/Cellar/neo4j/2.1.5/libexec/data/graph.db"
Map config = [
        "use_memory_mapped_buffers": "true",
        "neostore.nodestore.db.mapped_memory": "250M",
        "neostore.relationshipstore.db.mapped_memory": "1G",
        "neostore.propertystore.db.mapped_memory": "500M",
        "neostore.propertystore.db.strings.mapped_memory": "500M",
        "neostore.propertystore.db.arrays.mapped_memory": "0M",
        "cache_type": "none",
        "dump_config": "true"
]

String server = "http://brie.cshl.edu:3000/"
String path = 'taxonomy/select'
Integer start = 0
Integer rows = 5
JsonSlurper jsonSlurper = new JsonSlurper()

String url = sprintf('%1$s%2$s?start=%3$d&rows=%4$d', server, path, start, rows)
def contents = jsonSlurper.parse(url.toURL())
List results = contents.response

println results