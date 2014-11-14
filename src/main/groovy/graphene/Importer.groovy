package graphene

import graphene.loaders.*
import groovy.util.logging.Log4j2
import org.bson.types.ObjectId
import org.neo4j.graphdb.DynamicLabel
import org.neo4j.graphdb.Label
import org.neo4j.unsafe.batchinsert.BatchInserter
import org.neo4j.unsafe.batchinsert.BatchInserters

@Singleton
@Log4j2
class Importer {

    private final NodeCache nodeCache = NodeCache.instance
    private final LabelCache labelCache = LabelCache.instance
    private BatchInserter batch

    private static Set<Loader> dataLoaders = [new ReactomeLoader(), EOLoader.instance, GOLoader.instance,
                                              GROLoader.instance, POLoader.instance, SOLoader.instance,
                                              TOLoader.instance, NCBITaxonLoader.instance, DomainLoader.instance,
                                              GeneLoader.instance]
//    private
//    static Set<Loader> dataLoaders = [EOLoader.instance, GOLoader.instance, GROLoader.instance,
//                                      POLoader.instance, SOLoader.instance, TOLoader.instance,
//                                      NCBITaxonLoader.instance, DomainLoader.instance, GeneLoader.instance]

    static run(Map config, File dbLocation) {
        Importer.instance.go(config, dbLocation)
    }

    private go(Map config, File dbLocation) {
        try {
            batch = BatchInserters.inserter(dbLocation.canonicalPath, config)

            for (Loader l in dataLoaders) {
                String className = l.class.simpleName
                log.info "Loader $className starting"
                Long start = System.currentTimeMillis()
                l.load(batch, nodeCache, labelCache)
                log.info "$className took ${System.currentTimeMillis() - start} ms"
            }

            log.info "Adding indices"
            Long start = System.currentTimeMillis()
            indexOnNamePropertyForAllLabels();
            log.info "Indices took ${System.currentTimeMillis() - start} ms to add"
        }
        finally {
            batch?.shutdown()
            log.info "Shutdown."
        }
    }

    private indexOnNamePropertyForAllLabels() {
        Collection<Label> uniqueLabels = nodeCache.labelsWithUniqueNames()
        Collection<Label> nonUniqueLabels = labelCache.labels() - uniqueLabels

        for (Label l in uniqueLabels) {
            log.trace "Adding unique constraint to ${l.name()} on 'name'"
            batch.createDeferredConstraint(l).assertPropertyIsUnique('name').create()
            batch.createDeferredConstraint(l).assertPropertyIsUnique('id').create()
            batch.createDeferredConstraint(l).assertPropertyIsUnique('_id').create()
        }

        for (Label l in nonUniqueLabels) {
            log.trace "Indexing ${l.name()} on 'name'"
            batch.createDeferredSchemaIndex(l).on("name").create();
            batch.createDeferredSchemaIndex(l).on("id").create();
            batch.createDeferredSchemaIndex(l).on("_id").create();
        }

        for (Map.Entry<Label, Set<String>> labelIndices in Loader.labelIndicesToAdd) {
            Label l = labelIndices.key
            for(String prop in labelIndices.value) {
                log.info "create index on :$l($prop)"
                batch.createDeferredSchemaIndex(l).on(prop).create();
            }
        }
    }
}

@Log4j2
@Singleton
class NodeCache implements Map<Label, Map<String, Long>> {
    @Delegate
    Map<Label, Map<String, Long>> delegate = [:].withDefault { [:] }

//    Long create(Label label, Long id, Map props, BatchInserter batch) {
//        String name = props.name
//        if (!name) throw new RuntimeException("Need property name for $label node $id (props supplied $props)")
//        batch.createNode(id, props, label)
//    }

    Long getOrCreate(Label label, String name, BatchInserter batch) {
        Long result = delegate[label][name]
        if (result == null) {
            result = delegate[label][name] = batch.createNode([name: name], label)
        }
        result
    }

    Long augmentOrCreate(Label l, Map<String, ?> props, Collection<Label> labels, BatchInserter batch, String uniqueProp = 'name') {
        String name = props[uniqueProp]
        if (!name) {
            throw new RuntimeException("One property needs to be `name`. We got the following props: $props")
        }
        Long nodeId = getOrCreate(l, name, batch)

        addPropertiesToNode(props, batch, nodeId)
        setLabels(labels, l, batch, nodeId)
        return nodeId
    }

    private static void addPropertiesToNode(Map<String, ?> props, BatchInserter batch, long nodeId) {
        props = props.findAll { !(it.value instanceof Collection) }
                     .each { e -> if (e.value instanceof ObjectId) e.value = e.value.toString() }
        Map currentProps = batch.getNodeProperties(nodeId)
        currentProps.putAll(props)
        batch.setNodeProperties(nodeId, currentProps)
    }

    private static void setLabels(Collection<Label> labels, Label labelUsedForCaching, BatchInserter batch, long nodeId) {
        Label[] allLabels
        if (labels) {
            if (!labels.contains(labelUsedForCaching)) {
                allLabels = new Label[labels.size() + 1]
                allLabels[0] = labelUsedForCaching
                for (int i = 0; i < labels.size(); i++) {
                    allLabels[i + 1] = labels[i]
                }
            } else {
                allLabels = labels.toArray(new Label[labels.size()])
            }
            batch.setNodeLabels(nodeId, allLabels)
        }
    }

    Set<Label> labelsWithUniqueNames() { new LinkedHashSet<Label>(delegate.keySet()) }
}

@Log4j2
@Singleton
class LabelCache implements Map<String, Label> {
    @Delegate
    Map<String, Label> delegate = [:].withDefault { String s ->
        log.debug "Creating label $s"
        if (!s) {
            throw new RuntimeException("Don't try to get an empty or null label.")
        }
        DynamicLabel.label(s)
    }

    Set<Label> labels() {
        new LinkedHashSet<Label>(delegate.values())
    }

    Set<Label> getLabels(Collection<String> labels) {
        new LinkedHashSet<Label>(delegate.subMap(labels.findAll { it }).values())
    }
}