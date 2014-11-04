package graphene

import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.unsafe.batchinsert.BatchInserter

abstract class Loader {

    protected abstract void load()

    private BatchInserter batch
    private NodeCache nodes
    private LabelCache labels

    private Set<Rel> uncreatedRelationships = []
    private Map<?, Long> externalIdToNeoId = [:]

    void load(BatchInserter batchInserter, NodeCache nodeCache, LabelCache labelCache) {
        this.batch = batchInserter
        this.nodes = nodeCache
        this.labels = labelCache

        load()

        after()
    }

    void after() {
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

    // Only for Reactome because it's special. it must always be run first and its database ids are set to be neo's ids
    void nodeWithId(long externalId, Map nodeProps, Label... labels) {
        nodes.create(labels[0], externalId, nodeProps, batch)
        batch.setNodeLabels(externalId, labels)
    }

    long node(Label label, Map nodeProps, Label[] nodeLabels = []) {
        nodes.augmentOrCreate(label, nodeProps, nodeLabels, batch)
    }

    long node(long externalId, Label label, Map nodeProps, Label[] nodeLabels = []) {
        Long nodeId = node(label, nodeProps, nodeLabels)
        externalIdToNeoId[externalId] = nodeId
        nodeId
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


}