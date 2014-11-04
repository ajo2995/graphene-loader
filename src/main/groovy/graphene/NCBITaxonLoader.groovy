package graphene

import org.neo4j.graphdb.Label

import static graphene.Rels.ALT_ID
import static graphene.Rels.SUPER_TAXON

@Singleton
class NCBITaxonLoader extends GrameneMongoLoader implements Loader {

    @Override
    String getPath() { 'taxonomy' }

    @Override
    void process(Map taxon) {
//        taxon.remove('xref') // these don't seem to be useful

        List<String> xrefs = taxon.remove('xref')
        Label[] nodeLabels = labels.getLabels(['Taxon', taxon.rank, 'NCBITaxonomy'])
        Long taxonId = taxon._id
        Set<String> synonyms = taxon.remove('synonym')
        Long parentTaxonId = parentTaxonId(taxon)
        List<Long> altIds = taxon.remove('alt_id')

        Long nodeId = node(taxonId, labels.Taxon, taxon, nodeLabels)

        createSynonyms(nodeId, synonyms)
        if (parentTaxonId != null) link(nodeId, parentTaxonId, SUPER_TAXON)

        createXrefs(nodeId, xrefs)
        if (altIds) {
            for (Long altTaxonId in altIds) {
                link(nodeId, altTaxonId, ALT_ID)
            }
        }
    }

    static Long parentTaxonId(taxon) {
        def is_a = taxon.remove('is_a')
        if(is_a) {
            if(is_a instanceof Collection && is_a.size()) {
                return is_a[0]
            }
            else {
                return is_a
            }
        }
        null
    }

}
