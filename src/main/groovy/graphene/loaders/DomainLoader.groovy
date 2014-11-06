package graphene.loaders

import groovy.util.logging.Log4j2
import org.neo4j.graphdb.Label

@Singleton
@Log4j2
class DomainLoader extends GrameneMongoLoader {

    private static
    final Set<String> NODE_PROP_KEYS = ['_id', 'id', 'name', 'short_name', 'synonym', 'type', 'abstract'].asImmutable()

    @Override
    String getPath() { 'domains' }

    @Override
    void process(Map result) {
        Map nodeprops = result.subMap(NODE_PROP_KEYS).findAll { k, v -> v }
        Map<String, Object> domainDescriptors = result.findAll { k, v -> !NODE_PROP_KEYS.contains(k) }

        log.trace nodeprops

        long interproNodeId = node(labels.InterPro, nodeprops, [labels[nodeprops.type]])

        for (Map.Entry<String, Object> domain in domainDescriptors) {
            if (!(domain.value instanceof Collection)) {
                domain.value = Collections.singletonList(domain.value)
            }

            Label domainType = labels[domain.key]
            for (String domainName in domain.value) {
                long domainId = node(domainType, [name: domainName], [labels.InterProSignature, domainType])
                link(domainId, interproNodeId, Rels.CONTRIBUTES_TO)
            }
        }
    }


}
