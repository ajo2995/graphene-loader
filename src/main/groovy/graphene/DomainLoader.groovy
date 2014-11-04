package graphene

import groovy.util.logging.Log4j2

@Singleton
@Log4j2
class DomainLoader extends GrameneMongoLoader {

    private static final Set<String> NODE_PROP_KEYS = ['_id', 'id', 'name', 'short_name', 'synonym', 'type', 'abstract'].asImmutable()

    @Override
    String getPath() { 'domains' }

    @Override
    void process(Map result) {
        Map nodeprops = result.subMap(NODE_PROP_KEYS).findAll{ k,v -> v }
        Map domainDescriptors = result.findAll{ k, v -> !NODE_PROP_KEYS.contains(k) }

        long nodeId = node(nodeprops._id, labels.InterPro, nodeprops)
    }


}
