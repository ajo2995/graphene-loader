package graphene

import groovy.json.JsonSlurper
import groovy.transform.EqualsAndHashCode
import groovy.util.logging.Log4j
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.unsafe.batchinsert.BatchInserter

import java.util.regex.Matcher

import static graphene.Rels.SYNONYM
import static graphene.Rels.XREF

/**
 * Created by mulvaney on 10/31/14.
 */
@Log4j
abstract class GrameneMongoLoader implements Loader {

    static final String URL_TEMPLATE = 'http://brie.cshl.edu:3000/%1$s/select?start=%2$d&rows=%3$d'
    static final Integer ROWS = 200

    static final JsonSlurper JSON_SLURPER = new JsonSlurper()

    BatchInserter batch
    NodeCache nodes
    LabelCache labels

    private Set<Rel> relationships = []
    private Map<Long, Long> externalIdToNeoId = [:]

    abstract void process(Map result)
    abstract String getPath()

    long node(long externalId, Label label, Map nodeProps, Label[] nodeLabels) {
        Long nodeId = nodes.augmentOrCreate(label, nodeProps, nodeLabels, batch)
        externalIdToNeoId[externalId] = nodeId
        nodeId
    }

    void link(Long nodeId, Long parentExternalId, RelationshipType type) {
        Long parentId = externalIdToNeoId[parentExternalId]
        if (parentId != null && batch.nodeExists(parentId)) {
            batch.createRelationship(nodeId, parentId, type, Collections.emptyMap())
        } else {
            relationships.add(new Rel(fromNodeId: nodeId, toExternalId: parentExternalId, type: type))
        }
    }

    Long getNeoNodeId(Long taxonId) {
        externalIdToNeoId[taxonId]
    }

    private def parseJSON(Integer start) {
        URL url = createUrl(start)
        parseJSON(url)
    }

    static private def parseJSON(URL url) {
        JSON_SLURPER.parse(url)
    }

    private URL createUrl(Integer start) {
        sprintf(URL_TEMPLATE, getPath(), start, ROWS).toURL()
    }

    @Override
    void load(BatchInserter batchInserter, NodeCache nodeCache, LabelCache labelCache) {
        batch = batchInserter
        nodes = nodeCache
        labels = labelCache

        Integer start = 0

        while(true) {
            def contents = parseJSON(start)
            for (Map taxon in contents.response) {
               preprocess(taxon)
               process(taxon)
            }
            start += ROWS
            if(start > contents.count) break
        }

        after()
    }

    static preprocess(Map entry) {
        entry.remove('_terms')
        entry.remove('alt_id')
        entry.remove('ancestors')
        entry.remove('namespace')
        entry.synonym = getSynonyms(entry.remove('synonym'))

        Matcher rankMatcher = entry.remove('property_value') =~ /has_rank NCBITaxon:(\w+)/
        if (rankMatcher) {
            entry.rank = (rankMatcher[0][1])?.capitalize()
        }
    }

    void after() {
        for (Rel r in relationships) {
            Long parentId = getNeoNodeId(r.toExternalId)
            if (!batch.nodeExists(r.fromNodeId)) {
                throw new RuntimeException("Node $r.fromNodeId doesn't exist (from)")
            }
            if (parentId == null || !batch.nodeExists(parentId)) {
                log.info("Node $parentId doesn't exist (to). Ignoring relationship from $r.fromNodeId of type $r.type")
            } else {
                batch.createRelationship(r.fromNodeId, parentId, r.type, Collections.emptyMap())
            }
        }
    }


    static Set<String> getSynonyms(synonyms) {
        if (synonyms instanceof String) {
            synonyms = [synonyms]
        }
        synonyms as Set
    }

    void createSynonyms(Long nodeId, def synonyms) {
        // `synonyms` might be a scalar string or a list of strings

        Label nameLabel = labels.Name
        for (String s in synonyms) {
            long synonymNodeId = nodes.getOrCreate(nameLabel, s, batch)
            batch.createRelationship(nodeId, synonymNodeId, SYNONYM, Collections.emptyMap())
        }
    }

    static String underscoreCaseToCamelCase(String s) {
        s?.toLowerCase()?.split('_')*.capitalize()?.join('')
    }

    def createXrefs(long nodeId, List<String> xrefs) {
        for(String xref in xrefs) {
            if(xref.indexOf(':') > 0) {
                def (String key, String value) = xref.split(':', 2)
                if(key != 'GC_ID') createXref(key, value, nodeId)
            }
        }
    }

    def createXref(String type, String name, Long referrerId) {
        Label[] allLabels = labels.getLabels([type, 'Xref'])
        Map props = [name:name, type:type]
        if(type == 'Reactome' || type == 'VZ') {
            String[] splitt = props.name.split(' ', 2)
            props.name = splitt[0]
            if(splitt.length > 1) props.desc = splitt[1]
        }

        Long xrefId = node(referrerId, labels[type], props, allLabels)
        batch.createRelationship(referrerId, xrefId, XREF, Collections.emptyMap())
    }
}

enum Rels implements RelationshipType {
    SUPER_TAXON, ALT_ID, SYNONYM, XREF,
    INTERSECTION // logical intersection, see http://geneontology.org/page/ontology-structure search for 'cross-products'
}

// to store relations to nodes that don't exist yet
@EqualsAndHashCode
class Rel {
    long fromNodeId
    long toExternalId // we don't necessarily know the node id for the to side of a relationship. use GrameneMongoLoader#getNeoNodeId when all the nodes are loaded
    RelationshipType type
}
