package graphene.loaders

import groovy.util.logging.Log4j2
import org.neo4j.graphdb.DynamicRelationshipType
import org.neo4j.graphdb.Label

/**
 * Created by mulvaney on 11/6/14.
 */
@Singleton @Log4j2
class GeneLoader extends GrameneMongoLoader {

    // keep track of gene locations so that we can link them by adjacency
    // this map is keyed first by a concat of map and region, and then, in a TreeMap
    // by the gene start position.
    private Map<String, NavigableMap<Integer, Long>> adjacentGeneNodeIds = [:].withDefault { new TreeMap<>() }

    @Override
    String getPath() { 'genes' }

    @Override
    long process(Map gene) {
        log.trace gene

        gene = gene.findAll{ it.key && it.value }

        gene._id = gene._id.toString()
        Map<String, List> xrefs = gene.remove('xrefs') ?: Collections.emptyMap()
        Map<String, List<Long>> ontologyXrefs = xrefs.subMap('GO', 'TO', 'PO', 'EO', 'GRO', 'SO').findAll{ it.value }
        xrefs = xrefs.findAll{ !ontologyXrefs.containsKey(it.key) }
        Map<String, Object> location = gene.remove('location')
        gene.strand = location.strand
        Long taxonId = gene.remove('taxon_id')
        gene.remove('interpro')
        List<String> geneTrees = gene.remove('genetrees')
        Map<String, List<String>> proteinFeatures = gene.remove('protein_features') ?: Collections.emptyMap()

        long nodeId = nodeNoCache(gene, labels.Gene)

        linkToReactome(nodeId, gene.gene_id)
        linkToTaxon(nodeId, taxonId)
        createOntologyXrefs(nodeId, ontologyXrefs)
        createXrefs(nodeId, xrefs)
        createGenetrees(nodeId, geneTrees)
        createProteinFeatures(nodeId, proteinFeatures)
        createLocation(nodeId, location)
        addLocationToAdjacentsDatastructure(nodeId, location)

        nodeId
    }

    @Override
    void after() {
        super.after()

        for(String region in adjacentGeneNodeIds.keySet()) {
            NavigableMap<Integer, Long> adjacentNodes = adjacentGeneNodeIds[region]
            Map.Entry<Integer, Long> prevGeneLoc = adjacentNodes.firstEntry()

            for(Map.Entry<Integer, Long> geneLoc in adjacentNodes.tailMap(prevGeneLoc.key, false)) {
                link(prevGeneLoc.value, geneLoc.value, Rels.NEXT)
                prevGeneLoc = geneLoc
            }
        }
    }

    void addLocationToAdjacentsDatastructure(long nodeId, Map<String, Object> location) {
        String key = "$location.map::::$location.region"
        Integer start = location.start

        assert start < location.end
        adjacentGeneNodeIds[key][start] = nodeId
    }

    void linkToReactome(long nodeId, String geneIdentifier) {
        for(Label l in labels.getLabels(['DatabaseIdentifier', 'ReferenceEntity'])) {
            Long reactomeId = nodes[l][geneIdentifier]
            if(reactomeId) {
                link(reactomeId, nodeId, Rels.DATABASE_BRIDGE)
            }
        }
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
                    log.debug "Could not find node for $ontology $value"
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
            regionId = node(labels.Region, [name:location.map + ':' + location.region, regionName:location.region]) // oops, all chromosome 1s were the same.
        }

        if(createMapRegionRelationship) {
            link(mapId, regionId, Rels.CONTAINS)
        }

        link(nodeId, regionId, Rels.LOCATION, location.subMap('start', 'end', 'strand'))
    }

    void createProteinFeatures(long nodeId, Map<String, List<String>> features) {
        for(Map.Entry<String, List<String>> featureSet in features) {
            String feature = featureSet.key
            switch(feature) {
                case null:
                case '':
                    log.debug "Ignoring unnamed feature for node $nodeId"
                    break
                case { String feat -> DomainLoader.instance.isInterProSignature(feat) }:
                    log.debug "Ignoring protein feature that is an interpro signature"
                    break
                case 'interpro':
                    List<Integer> iprIds = featureSet.value.sort()
                    String iprSetName = iprIds.collect{ String.format('IPR%06d', it) }.join('; ')
                    boolean linkDomainsToSet = getNodeId(iprSetName) == null // if the node does not exist we need to also link to interpro domains
                    Long setNodeId = node(iprSetName, labels.InterProSet, [name: iprSetName])
                    for (Integer interproId in featureSet.value) {
                        Long interproNodeId = DomainLoader.instance.getNodeId(interproId)
                        if (interproNodeId) {
                            link(nodeId, interproNodeId, Rels.CONTAINS)
                            if(linkDomainsToSet) {
                                link(setNodeId, interproNodeId, Rels.CONTAINS)
                            }
                            link(setNodeId, nodeId, Rels.CONTAINS)
                        }
                        else
                            log.debug "Could not find interpro id $interproId"
                    }
                    break
                default:
                    for (String featureVal in featureSet.value) {
                        long id = node(featureVal, labels[feature], [name: featureVal], labels.getLabels([feature, 'ProteinFeature']))
                        link(nodeId, id, Rels.CONTAINS)
                    }
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
