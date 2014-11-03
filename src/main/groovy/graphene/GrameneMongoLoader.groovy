package graphene

import groovy.json.JsonSlurper
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.unsafe.batchinsert.BatchInserter

import static graphene.Rels.SYNONYM

/**
 * Created by mulvaney on 10/31/14.
 */
abstract class GrameneMongoLoader implements Loader {

    static final String SERVER = "http://brie.cshl.edu:3000/"
    static final Integer ROWS = 200

    static final JsonSlurper JSON_SLURPER = new JsonSlurper()

    BatchInserter batch
    NodeCache nodes
    LabelCache labels

    private List<Rel> relationships = []
    private Map<Long, Long> externalIdToNeoId = [:]

    abstract void process(Map result)

    long node(long taxonId, Label label, Map taxon, Label[] nodeLabels) {
        Long nodeId = nodes.augmentOrCreate(label, taxon, nodeLabels, batch)
        externalIdToNeoId[taxonId] = nodeId
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

    def parseJSON(String path, Integer start) {
        URL url = createUrl(path, start)
        parseJSON(url)
    }

    static def parseJSON(URL url) {
        JSON_SLURPER.parse(url)
    }

    URL createUrl(String path, Integer start) {
        sprintf('%1$s%2$s?start=%3$d&rows=%4$d', SERVER, path, start, ROWS).toURL()
    }

    @Override
    void load(BatchInserter batchInserter, NodeCache nodeCache, LabelCache labelCache) {
        batch = batchInserter
        nodes = nodeCache
        labels = labelCache

        Integer start = 0

        while(true) {
            def contents = parseJSON(path, start)
            for (Map taxon in contents.response) {
               process(taxon)
            }
            start += ROWS
            if(start > contents.count) break
        }

        after()
    }

    void after() {
        for (Rel r in relationships) {
            Long parentId = getNeoNodeId(r.toExternalId)
            if (!batch.nodeExists(r.fromNodeId)) {
                throw new RuntimeException("Node $r.fromNodeId doesn't exist (from)")
            }
            if (parentId == null || !batch.nodeExists(parentId)) {
                System.out.println("Node $parentId doesn't exist (to). Ignoring relationship from $r.fromNodeId of type $r.type")
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
}

enum Rels implements RelationshipType {
    SUPER_TAXON, ALT_ID, SYNONYM
}

// to store relations to nodes that don't exist yet
class Rel {
    long fromNodeId
    long toExternalId // we don't necessarily know the node id for the to side of a relationship. use GrameneMongoLoader#getNeoNodeId when all the nodes are loaded
    RelationshipType type
}
