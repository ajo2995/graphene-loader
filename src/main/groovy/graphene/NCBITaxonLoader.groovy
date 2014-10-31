package graphene

import groovy.json.JsonSlurper
import org.neo4j.graphdb.DynamicRelationshipType
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.unsafe.batchinsert.BatchInserter

import java.util.regex.Matcher

@Singleton
class NCBITaxonLoader implements Loader {

    public static final DynamicRelationshipType SUPERTAXON_REL_TYPE = DynamicRelationshipType.withName('SUPER_TAXON')
    public static final DynamicRelationshipType ALT_ID_TYPE = DynamicRelationshipType.withName('ALT_ID')
    public static final DynamicRelationshipType SYNONYM_TYPE = DynamicRelationshipType.withName('SYNONYM')

    final String server = "http://brie.cshl.edu:3000/"
    final String path = 'taxonomy/select'
    final Integer rows = 200

    JsonSlurper jsonSlurper = new JsonSlurper()

    BatchInserter batch
    NodeCache nodes
    LabelCache labels

    List<Rel> relationships = []
    Map<Long, Long> taxonIdToNeoId = [:]

    @Override
    void load(BatchInserter batchInserter, NodeCache nodeCache, LabelCache labelCache) {
        batch = batchInserter
        nodes = nodeCache
        labels = labelCache

        Integer start = 0

        while(true) {
            String url = sprintf('%1$s%2$s?start=%3$d&rows=%4$d', server, path, start, rows)
            def contents = jsonSlurper.parse(url.toURL())
            for (Map taxon in contents.response) {
                preprocessTaxon(taxon)
                createOrAugmentTaxon(taxon)
            }
            start += rows
            if(start > contents.count) break
        }

        for(Rel r in relationships) {
            Long parentId = taxonIdToNeoId[r.to]
            if(!batch.nodeExists(r.from)) {
                throw new RuntimeException("Node $r.from doesn't exist (from)")
            }
            if(parentId == null || !batch.nodeExists(parentId)) {
                System.out.println("Node $parentId doesn't exist (to). Ignoring relationship from $r.from of type $r.type")
            }
            else {
                batch.createRelationship(r.from, parentId, r.type, Collections.emptyMap())
            }
        }
    }

    void createOrAugmentTaxon(Map taxon) {
        Label[] nodeLabels = labels.subMap(['Taxon', taxon.rank, 'NCBITaxonomy'].findAll{it}).values() as Label[]
        Long taxonId = taxon.taxonId
        taxon.remove('xref') // these don't seem to be useful
        Set<String> synonyms = taxon.remove('synonym')
        Long parentTaxonId = taxon.remove('is_a')
        List<Long> altIds = taxon.remove('alt_id')

        Long nodeId = nodes.augmentOrCreate(labels.Taxon, taxon, nodeLabels, batch)
        taxonIdToNeoId[taxonId] = nodeId

        createSynonyms(nodeId, synonyms)
        if(parentTaxonId != null) link(nodeId, parentTaxonId, SUPERTAXON_REL_TYPE)

        if(altIds) {
            for(Long altTaxonId in altIds) {
                link(nodeId, altTaxonId, ALT_ID_TYPE)
            }
        }

    }

    static Long getTaxonId(def taxonId) {
        if(taxonId instanceof Collection) {
            taxonId = taxonId[0]
        }
        taxonId
    }

    static Set<String> getSynonyms(synonyms) {
        if(synonyms instanceof String) {
            synonyms = [synonyms]
        }
        synonyms as Set
    }

    void createSynonyms(Long nodeId, def synonyms) {
        // `synonyms` might be a scalar string or a list of strings

        Label nameLabel = labels.Name
        for(String s in synonyms) {
            long synonymNodeId = nodes.getOrCreate(nameLabel, s, batch)
            batch.createRelationship(nodeId, synonymNodeId, SYNONYM_TYPE, Collections.emptyMap())
        }
//    batch.
    }

    void link(Long nodeId, Long parentTaxonId, RelationshipType type) {
        Long parentId = taxonIdToNeoId[parentTaxonId]
        if(parentId != null && batch.nodeExists(parentId)) {
            batch.createRelationship(nodeId, parentId, type, Collections.emptyMap())
        }
        else {
            relationships.add(new Rel(from: nodeId, to:parentTaxonId, type:type))
        }
    }

    static preprocessTaxon(Map taxon) {
        taxon.remove('_terms')
        taxon.remove('ancestors')
        taxon.remove('namespace')
        taxon.synonym = getSynonyms(taxon.remove('synonym'))
        taxon.taxonId = getTaxonId(taxon.remove('_id'))
        taxon.is_a = getTaxonId(taxon.remove('is_a'))

//        for(Map.Entry e in taxon) {
//            // if it's a single-entry array then yay
//            if(e.value instanceof Collection && e.value.size() == 1) {
//                e.value = e.value[0]
//            }
//        }

        Matcher rankMatcher = taxon.remove('property_value') =~ /has_rank NCBITaxon:(\w+)/
        if (rankMatcher) {
            taxon.rank = (rankMatcher[0][1])?.capitalize()
        }
    }
}
