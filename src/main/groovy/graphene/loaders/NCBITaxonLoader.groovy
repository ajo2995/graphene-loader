package graphene.loaders

import groovy.util.logging.Log4j2
import org.neo4j.graphdb.Label

import static Rels.ALT_ID
import static Rels.SUPER_TAXON

@Singleton
@Log4j2
class NCBITaxonLoader extends GrameneMongoLoader {

    @Override
    String getPath() { 'taxonomy' }

    @Override
    long process(Map taxon) {
        List<String> xrefs = taxon.remove('xref')
        Collection<Label> nodeLabels = labels.getLabels(['Taxon', taxon.rank, 'NCBITaxonomy'])
        Long taxonId = taxon._id
        Set<String> synonyms = taxon.remove('synonym')
        Long parentTaxonId = parentTaxonId(taxon)
        List<Long> altIds = taxon.remove('alt_id')

        Long nodeId = node(taxonId, labels.Taxon, taxon, nodeLabels)

        createSynonyms(nodeId, synonyms)
        if (parentTaxonId != null) linkToExternal(nodeId, parentTaxonId, SUPER_TAXON)

        createXrefs(nodeId, xrefs)
        if (altIds) {
            for (Long altTaxonId in altIds) {
                linkToExternal(nodeId, altTaxonId, ALT_ID)
            }
        }

        nodeId
    }

    static Long parentTaxonId(taxon) {
        def is_a = taxon.remove('is_a')
        if (is_a) {
            if (is_a instanceof Collection && is_a.size()) {
                return is_a[0]
            } else {
                return is_a
            }
        }
        null
    }

}
