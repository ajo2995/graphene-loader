package graphene

import org.neo4j.graphdb.Label

import java.util.regex.Matcher

import static graphene.Rels.*

@Singleton
class NCBITaxonLoader extends GrameneMongoLoader implements Loader {

    final String path = 'taxonomy/select'

    @Override
    void process(Map taxon) {
        preprocessTaxon(taxon)
        createOrAugmentTaxon(taxon)
    }

    static preprocessTaxon(Map taxon) {
        taxon.remove('_terms')
        taxon.remove('ancestors')
        taxon.remove('namespace')
        taxon.remove('xref') // these don't seem to be useful
        taxon.synonym = getSynonyms(taxon.remove('synonym'))
        taxon.taxonId = getTaxonId(taxon.remove('_id'))
        taxon.is_a = getTaxonId(taxon.remove('is_a'))

        Matcher rankMatcher = taxon.remove('property_value') =~ /has_rank NCBITaxon:(\w+)/
        if (rankMatcher) {
            taxon.rank = (rankMatcher[0][1])?.capitalize()
        }
    }

    void createOrAugmentTaxon(Map taxon) {
        Label[] nodeLabels = labels.subMap(['Taxon', taxon.rank, 'NCBITaxonomy'].findAll { it }).values() as Label[]
        Long taxonId = taxon.taxonId
        Set<String> synonyms = taxon.remove('synonym')
        Long parentTaxonId = taxon.remove('is_a')
        List<Long> altIds = taxon.remove('alt_id')

        Long nodeId = node(taxonId, labels.Taxon, taxon, nodeLabels)

        createSynonyms(nodeId, synonyms)
        if (parentTaxonId != null) link(nodeId, parentTaxonId, SUPER_TAXON)

        if (altIds) {
            for (Long altTaxonId in altIds) {
                link(nodeId, altTaxonId, ALT_ID)
            }
        }
    }

    static Long getTaxonId(def taxonId) {
        if (taxonId instanceof Collection) {
            taxonId = taxonId[0]
        }
        taxonId
    }

}
