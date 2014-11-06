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

        String geneId = gene._id
        Map<String, List<String>> xrefs = gene.remove('xrefs') ?: Collections.emptyMap()
        gene.remove('domains') // TODO use this
        gene.remove('location') // TODO when andrew's done this
        List<String> geneTrees = gene.remove('genetrees')

        long nodeId = node(geneId, labels.Gene, gene)

        createXrefs(nodeId, xrefs)
        createGenetrees(nodeId, geneTrees)
    }

    void createGenetrees(long geneId, List<String> geneTrees) {
        for(String geneTree in geneTrees) {
            long geneTreeId = node(labels.GeneTree, [name: geneTree])
            link(geneId, geneTreeId, DynamicRelationshipType.withName('IN'))
        }
    }
}
