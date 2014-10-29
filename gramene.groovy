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


