package graphene.loaders

import graphene.LabelCache
import graphene.NodeCache
import groovy.transform.EqualsAndHashCode
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.unsafe.batchinsert.BatchInserter

abstract class Loader {

    protected abstract void load()

    private BatchInserter batch
    private NodeCache nodes
    private LabelCache labels

    private Set<Rel> uncreatedRelationships = []
    private Map<Object, Long> externalIdToNeoId = [:]

    void load(BatchInserter batchInserter, NodeCache nodeCache, LabelCache labelCache) {
        this.batch = batchInserter
        this.nodes = nodeCache
        this.labels = labelCache

        load()

        after()
    }

    void after() {
        addDeferredRelationships()
    }

    private addDeferredRelationships() {
        for (Rel r in uncreatedRelationships) {
            Long parentId = externalIdToNeoId[r.toExternalId]
            if (!batch.nodeExists(r.fromNodeId)) {
                throw new RuntimeException("Node $r.fromNodeId doesn't exist (from)")
            }
            if (parentId == null || !batch.nodeExists(parentId)) {
                log.error("Node $parentId doesn't exist (to). Ignoring relationship from $r.fromNodeId of type $r.type")
            } else {
                link(r.fromNodeId, parentId, r.type)
            }
        }
    }

    long node(Label label, Map nodeProps, Collection<Label> nodeLabels = []) {
        nodes.augmentOrCreate(label, nodeProps, nodeLabels, batch)
    }

    // Only for Reactome because it's special: It must always be run first because its database ids are set to be neo's ids
    void nodeNoCache(long externalId, Map nodeProps, Label... labels) {
        batch.createNode(externalId, nodeProps, labels)
    }

    long nodeNoCache(Map nodeProps, Label... labels) {
        batch.createNode(nodeProps, labels)
    }

    long node(externalId, Label label, Map nodeProps, Collection<Label> nodeLabels = []) {
        Long nodeId = node(label, nodeProps, nodeLabels)
        externalIdToNeoId[externalId] = nodeId
        nodeId
    }

    Long getNodeId(long externalId) {
        return externalIdToNeoId[externalId]
    }

    long link(Long from, Long to, RelationshipType type, Map relProps = Collections.emptyMap()) {
        batch.createRelationship(from, to, type, relProps)
    }

    void linkToExternal(Long nodeId, parentExternalId, RelationshipType type) {
        Long parentId = externalIdToNeoId[parentExternalId]
        if (parentId != null && batch.nodeExists(parentId)) {
            link(nodeId, parentId, type)
        } else {
            uncreatedRelationships.add(new Rel(fromNodeId: nodeId, toExternalId: parentExternalId, type: type))
        }
    }

    protected BatchInserter getBatch() {
        return batch
    }

    protected NodeCache getNodes() {
        return nodes
    }

    protected LabelCache getLabels() {
        return labels
    }

    protected setNodeProperty(long id, name, value) {
        Map nodeProps = batch.getNodeProperties(id)
        nodeProps[name] = value
        batch.setNodeProperties(id, nodeProps)
    }
}

enum Rels implements RelationshipType {
    SUPER_TAXON, ALT_ID, SYNONYM, XREF,
    INTERSECTION, // logical intersection, see http://geneontology.org/page/ontology-structure search for 'cross-products'
    CONTRIBUTES_TO, CONTAINS, SPECIES, LOCATION
}

// to store relations to nodes that don't exist yet
@EqualsAndHashCode
class Rel {
    long fromNodeId
    def toExternalId
    // we don't necessarily know the node id for the to side of a relationship. use GrameneMongoLoader#getNeoNodeId when all the nodes are loaded
    RelationshipType type
}