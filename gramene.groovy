import groovy.json.JsonSlurper
import org.neo4j.graphdb.DynamicLabel
import org.neo4j.graphdb.Label
import org.neo4j.unsafe.batchinsert.BatchInserter
import org.neo4j.unsafe.batchinsert.BatchInserters

import java.util.regex.Matcher

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

BatchInserter batch = BatchInserters.inserter(store, config)
// Keep track of all the labels that we create. This is important because we need to add indices to them at the end.
Map<String, Label> labelCache = [:].withDefault { String s -> s ? DynamicLabel.label(s) : null }

try {
    final String server = "http://brie.cshl.edu:3000/"
    final String path = 'taxonomy/select'
    final Integer rows = 5
    Integer start = 0
    JsonSlurper jsonSlurper = new JsonSlurper()

    String url = sprintf('%1$s%2$s?start=%3$d&rows=%4$d', server, path, start, rows)
    def contents = jsonSlurper.parse(url.toURL())
    for (Map taxon in contents.response) {
        preprocessTaxon(taxon)
        createOrAugmentTaxon(batch, labelCache, taxon)
    }

    println contents.response
} finally {
    batch.shutdown()
}

void createOrAugmentTaxon(BatchInserter batch, Map<String, Label> labelCache, Map taxon) {
    Collection<Label> labels = labelCache.subMap(['Taxon', taxon.rank]).values().findAll{it}
    println labels
}

void createSynonyms(BatchInserter batch, Map<String, Label> labelCache, List<String> synonyms) {
    Label nameLabel = labelCache.Name
    batch.
}

void preprocessTaxon(Map taxon) {
    taxon.remove('_terms')
    taxon.remove('ancestors')
    taxon.remove('namespace')
    if (taxon.synonym) {
        taxon.synonym = taxon.synonym as Set
    }

    Matcher rankMatcher = taxon.property_value =~ /has_rank NCBITaxon:(\w+)/
    if (rankMatcher) {
        taxon.rank = (rankMatcher[0][1])?.capitalize()
    }
}