package graphene.loaders

import groovy.util.logging.Log4j2
import org.neo4j.graphdb.DynamicRelationshipType

/**
 * Created by mulvaney on 11/6/14.
 */
@Singleton @Log4j2
class GeneLoader extends GrameneMongoLoader {

    @Override
    String getPath() { 'genes' }

    @Override
    void process(Map gene) {
        log.trace gene

        gene = gene.findAll{ it.key && it.value }

        gene._id = gene._id.toString()
        Map<String, List> xrefs = gene.remove('xrefs') ?: Collections.emptyMap()
        Map<String, List<Long>> ontologyXrefs = xrefs.subMap('GO', 'TO', 'PO', 'EO', 'GRO', 'SO').findAll{ it.value }
        xrefs = xrefs.findAll{ !ontologyXrefs.containsKey(it.key) }
        Map<String, Object> location = gene.remove('location')
        Long taxonId = gene.remove('taxon_id')
        gene.remove('interpro')
        List<String> geneTrees = gene.remove('genetrees')
        Map<String, List<String>> proteinFeatures = gene.remove('protein_features') ?: Collections.emptyMap()

        long nodeId = nodeNoCache(gene, labels.Gene)

        linkToTaxon(nodeId, taxonId)
        createOntologyXrefs(nodeId, ontologyXrefs)
        createXrefs(nodeId, xrefs)
        createGenetrees(nodeId, geneTrees)
        createProteinFeatures(nodeId, proteinFeatures)
        createLocation(nodeId, location)
    }

    void createOntologyXrefs(long nodeId, Map<String, List> ontologyXrefs) {
        for(Map.Entry<String, List> ont in ontologyXrefs) {
            String ontology = ont.key
            Class<Loader> loaderClass = Class.forName("graphene.loaders.${ontology}Loader")
            Loader loader = loaderClass.instance

            for(Long value in ont.value) {
                Long ontId = loader.getNodeId(value)
                if(ontId) {
                    link(nodeId, ontId, Rels.XREF)
                }
                else {
                    log.info "Could not find node for $ontology $value"
                }
            }

        }
    }

    void linkToTaxon(long geneId, long taxonExternalId) {
        Long taxonNodeId = NCBITaxonLoader.instance.getNodeId(taxonExternalId)
        if(taxonNodeId) link(geneId, taxonNodeId, Rels.SPECIES)
        else log.error "No taxon found for $taxonExternalId"
    }

    void createLocation(long nodeId, Map<String, Object> location) {
        Long mapId = nodes[labels.Map][location.map]
        Long regionId = nodes[labels.Region][location.region]
        boolean createMapRegionRelationship = !(mapId && regionId)

        if(!mapId) {
            mapId = node(labels.Map, [name:location.map])
        }
        if(!regionId) {
            regionId = node(labels.Region, [name:location.region])
        }

        if(createMapRegionRelationship) {
            link(mapId, regionId, Rels.CONTAINS)
        }

        link(nodeId, regionId, Rels.LOCATION, location.subMap('start', 'end', 'strand'))
    }

    void createProteinFeatures(long nodeId, Map<String, List<String>> features) {
        for(Map.Entry<String, List<String>> featureSet in features) {
            String feature = featureSet.key
            if(!feature) {
                log.error "Unnamed feature for node $nodeId"
                continue
            }
            if(feature == 'interpro') feature = 'InterPro'
            for(String featureVal in featureSet.value) {
                long id = node(featureVal, labels[feature], [name: featureVal], labels.getLabels([feature, 'ProteinFeature']))
                link(nodeId, id, Rels.CONTAINS)
            }
        }
    }

    void createGenetrees(long geneId, List<String> geneTrees) {
        for(String geneTree in geneTrees) {
            long geneTreeId = node(labels.GeneTree, [name: geneTree])
            link(geneId, geneTreeId, DynamicRelationshipType.withName('IN'))
        }
    }
}
