package graphene

import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.DynamicRelationshipType
import org.neo4j.graphdb.Label

import java.util.regex.Matcher

/**
 * Created by mulvaney on 11/3/14.
 */
@Singleton
class GOLoader extends GrameneMongoLoader {


    public static final String GO_RELATIONSHIP_PATTERN = /([a-z_]+) GO:0*(\d+) ! (.*)/

    @Override
    String getPath() { 'GO' }

    @Override
    void process(Map goNode) {
        goNode.remove('def') // it's a bit long

        Long id = goNode.remove('_id')

        if(goNode.is_obsolete) {
            println "Ignoring obsolete node $goNode"
            return;
        }

        if(id == null) {
            throw new RuntimeException("Node $goNode has no external id")
        }

        String namespace = namespace(goNode.remove('namespace'))
        List<String> subsets = goNode.remove('subset')

        Label[] goLabels = getGoNodeLabels(namespace, subsets)
//        Collection<Long> parentExternalIds = goNode.remove('is_a')
        Set<String> synonyms = goNode.remove('synonym')
        List<String> xrefs = goNode.remove('xref')
        List<String> relationships = goNode.remove('relationship')
        List intersections = goNode.remove('intersection_of')
        List<String> subset = goNode.remove('subset')
        Map<String, Collection<Long>> otherRels = findOtherRelations(goNode)

        Long nodeId = node(id, labels.GO, goNode, goLabels)

        createSynonyms(nodeId, synonyms)
        createXrefs(nodeId, xrefs)
        createIntersections(nodeId, intersections)
        createRelationships(nodeId, relationships)
        createSubsets(nodeId, subset)
        createOtherRels(nodeId, otherRels)

    }

    void createOtherRels(long nodeId, Map<String, Collection<Long>> rels) {
        rels.each{ String relName, Collection<Long> relGoIds ->
            RelationshipType relType = DynamicRelationshipType.withName(relName.toUpperCase())
            for(Long relGoId in relGoIds) {
                link(nodeId, relGoId, relType)
            }
        }
    }

    Map<String, Collection<Long>> findOtherRelations(Map<String, ?> goNode) {
        goNode.findAll{ String k, v -> v instanceof Collection }.each{ goNode.remove(it.key) }
    }

    Label[] getGoNodeLabels(namespace, subsets) {
        List labelStrings = [ 'GO', namespace ]
        if(subsets) {
            for(String subset in subsets) {
                String labelName = underscoreCaseToCamelCase(subset)
                labelStrings << labelName
            }
        }
        labels.getLabels(labelStrings)
    }

    void createSubsets(long l, List<String> subsets) {

    }
// dynamic relationship types between nodes.
    void createRelationships(long nodeId, Object relationships) {
        for(String rship in relationships) {
            createRelationshipFromString(rship, nodeId)
        }
    }

    // logical intersection, see http://geneontology.org/page/ontology-structure search for 'cross-products'
    def createIntersections(long nodeId, List intersections) {
        if(!intersections) return
        def (Long id, String name) = intersections
        link(nodeId, id, Rels.INTERSECTION)
        createRelationshipFromString(name, nodeId)
    }


    private createRelationshipFromString(String rship, long nodeId) {
        if (rship ==~ GO_RELATIONSHIP_PATTERN) {
            def (_, String type, String goIdStr) = Matcher.lastMatcher[0]
            Long goId = Long.valueOf(goIdStr, 10)
            RelationshipType relType = DynamicRelationshipType.withName(type.toUpperCase())
            link(nodeId, goId, relType)
        }
    }
    String namespace(String namespace) {
        if(namespace) {
            namespace = underscoreCaseToCamelCase(namespace)
            return labels[namespace]
        }
    }
}
