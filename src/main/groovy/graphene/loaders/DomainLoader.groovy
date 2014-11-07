package graphene.loaders

import groovy.util.logging.Log4j2
import org.neo4j.graphdb.Label

@Singleton
@Log4j2
class DomainLoader extends GrameneMongoLoader {

    private static
    final Set<String> NODE_PROP_KEYS = ['_id', 'id', 'name', 'short_name', 'synonym', 'type', 'description', 'abstract'].asImmutable()

    private final Set<String> signatureTypes = []

    @Override
    String getPath() { 'domains' }

    @Override
    long process(Map result) {
        Map nodeprops = result.subMap(NODE_PROP_KEYS).findAll { k, v -> v }
        Map<String, Object> domainDescriptors = result.findAll { k, v -> !NODE_PROP_KEYS.contains(k) }

        // let's keep track of all the types of signature. We'll use this in GeneLoader to not add protein_features that are also reffed by interpro
        signatureTypes.addAll(domainDescriptors.keySet()*.toLowerCase())

        log.trace nodeprops

        long interproNodeId = node(nodeprops._id, labels.InterPro, nodeprops, [labels[nodeprops.type]])

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

        interproNodeId
    }

    boolean isInterProSignature(String name) {
        signatureTypes.contains name?.toLowerCase()
    }

}
