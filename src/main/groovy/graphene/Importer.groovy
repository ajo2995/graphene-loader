package graphene

import groovy.util.logging.Log4j
import org.neo4j.graphdb.DynamicLabel
import org.neo4j.graphdb.Label
import org.neo4j.unsafe.batchinsert.BatchInserter
import org.neo4j.unsafe.batchinsert.BatchInserters

@Log4j
class Importer {

    private final NodeCache nodeCache = NodeCache.instance
    private final LabelCache labelCache = LabelCache.instance
    private final BatchInserter batch

    private Set<Loader> dataLoaders = [new ReactomeLoader(), NCBITaxonLoader.instance, GOLoader.instance, TOLoader.instance,
                                        POLoader.instance, SOLoader.instance, EOLoader.instance,
                                        GROLoader.instance]

    public Importer(Map config, File dbLocation) {
        try {
            batch = BatchInserters.inserter(dbLocation.canonicalPath, config)

            for(Loader l in dataLoaders) {
                l.load(batch, nodeCache, labelCache)
            }

            indexOnNamePropertyForAllLabels();
        }
        finally {
            batch?.shutdown()
            log.info "Shutdown."
        }
    }

    private indexOnNamePropertyForAllLabels() {
        for (Label l in labelCache.values()) {
            log.info "Indexing ${l.name()} on 'name'"
            batch.createDeferredSchemaIndex(l).on("name").create();
        }
    }

    public static void main(args) {

    }
}

@Log4j
@Singleton
class NodeCache implements Map<Label, Map<String, Long>> {
    @Delegate Map<Label, Map<String, Long>> delegate = [:].withDefault { [:] }

    Long getOrCreate(Label label, String name, BatchInserter batch) {
        Long result = delegate[label][name]
        if(result == null) {
            result = delegate[label][name] = batch.createNode([name:name], label)
        }
        result
    }

    Long augmentOrCreate(Label l, Map<String, ?> props, Label[] allLabels, BatchInserter batch) {
        if(!props?.name) {
            throw new RuntimeException('One property needs to be `name`. We got the following props: ${props?.keySet()}')
        }
        String name = props.name
        Long result = getOrCreate(l, name, batch)
        for(def prop in props.keySet()) {
            def val = props[prop]
            if(val instanceof Collection) {
                log.info "Can't add a Collection as a property. We got $val with key $prop"
            }
            else {
                batch.setNodeProperty(result, prop, val)
            }
        }
        if(allLabels) batch.setNodeLabels(result, allLabels)
        return result
    }
}
@Log4j
@Singleton
class LabelCache implements Map<String, Label> {
    @Delegate Map<String, Label> delegate = [:].withDefault { String s ->
        log.info "Creating label $s"
        if(!s) {
            throw new RuntimeException("Don't try to get an empty or null label.")
        }
        DynamicLabel.label(s)
    }

    Label[] getLabels(Collection<String> labels) {
        this.subMap(labels.findAll { it }).values() as Label[]
    }
}