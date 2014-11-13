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
    private Map<Long, NavigableMap<Integer, Long>> adjacentGeneNodeIds = [:].withDefault { new TreeMap<>() }

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
        gene.start = location.start
        gene.end = location.end
        gene.strand = location.strand
        Long taxonId = gene.remove('taxon_id')
        gene.remove('interpro')
        List<String> geneTrees = gene.remove('genetrees')
        Map<String, List<String>> proteinFeatures = gene.remove('protein_features') ?: Collections.emptyMap()

        long nodeId = nodeNoCache(gene, labels.Gene)

        linkToReactome(nodeId, gene.gene_id)
        Long taxonNodeId = linkToTaxon(nodeId, taxonId)
        createOntologyXrefs(nodeId, ontologyXrefs)
        createXrefs(nodeId, xrefs)
        createGenetrees(nodeId, geneTrees)
        createProteinFeatures(nodeId, proteinFeatures)
        createLocation(nodeId, taxonNodeId, location)

        nodeId
    }

    @Override
    void after() {
        super.after()

        for(Long regionId in adjacentGeneNodeIds.keySet()) {
            NavigableMap<Integer, Long> adjacentNodes = adjacentGeneNodeIds[regionId]
            Map.Entry<Integer, Long> prevGeneLoc = adjacentNodes.firstEntry()
            link(regionId, prevGeneLoc.value, Rels.FIRST_GENE)

            for(Map.Entry<Integer, Long> geneLoc in adjacentNodes.tailMap(prevGeneLoc.key, false)) {
                link(prevGeneLoc.value, geneLoc.value, Rels.NEXT)
                prevGeneLoc = geneLoc
            }

            link(regionId, prevGeneLoc.value, Rels.LAST_GENE)
        }
    }

    void addLocationToAdjacentsDatastructure(long nodeId, long regionId, Integer start) {
        adjacentGeneNodeIds[regionId][start] = nodeId
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
                    incrementNodeProperty(ontId, 'geneCount')
                    link(nodeId, ontId, Rels.XREF)
                }
                else {
                    log.debug "Could not find node for $ontology $value"
                }
            }
        }
    }

    Long linkToTaxon(long geneId, long taxonExternalId) {
        Long taxonNodeId = NCBITaxonLoader.instance.getNodeId(taxonExternalId)
        if(taxonNodeId) link(geneId, taxonNodeId, Rels.SPECIES)
        else log.error "No taxon found for $taxonExternalId"
        return taxonNodeId
    }

    void createLocation(long nodeId, Long taxonId, Map<String, Object> location) {
        final String uniqueMapName = location.map
        final String uniqueRegionName = location.map + ':' + location.region
        Long mapId = nodes[labels.Map][uniqueMapName]
        Long regionId = nodes[labels.Region][uniqueRegionName]

        if(!mapId) {
            mapId = node(uniqueMapName, labels.Map)
            if(taxonId != null) link(taxonId, mapId, Rels.CONTAINS)
        }
        if(!regionId) {
            regionId = node(uniqueRegionName, labels.Region, [name:uniqueRegionName, regionName:location.region]) // oops, all chromosome 1s were the same.
            link(mapId, regionId, Rels.CONTAINS)
            incrementNodeProperty(mapId, 'regionCount')
        }

        link(nodeId, regionId, Rels.LOCATION)
        incrementNodeProperty(regionId, 'geneCount')

        addLocationToAdjacentsDatastructure(nodeId, regionId, location.start)
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
