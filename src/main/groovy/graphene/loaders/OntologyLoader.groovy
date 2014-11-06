package graphene.loaders

import groovy.util.logging.Log4j2
import org.neo4j.graphdb.DynamicRelationshipType
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType

import java.util.regex.Matcher

/**
 * Created by mulvaney on 11/3/14.
 */
@Log4j2
abstract class OntologyLoader extends GrameneMongoLoader {

    private final String ONTOLOGY_RELATIONSHIP_PATTERN = /([a-z_]+) $path:0*(\d+) ! (.*)/

    @Override
    void process(Map oNode) {
        oNode.remove('def') // it's a bit long

        Long id = oNode['_id']

        if (oNode.is_obsolete) {
            log.trace "Ignoring obsolete node $oNode"
            return;
        }

        if (id == null) {
            throw new RuntimeException("Node $oNode has no external id")
        }

        String namespace = namespace(oNode.remove('namespace'))
        List<String> subsets = oNode.remove('subset')

        Collection<Label> oLabels = getOntologyNodeLabels(namespace, subsets)
        Set<String> synonyms = oNode.remove('synonym')
        List<String> xrefs = oNode.remove('xref') ?: Collections.emptyList()
        List<String> relationships = oNode.remove('relationship')
        List intersections = oNode.remove('intersection_of')
        List<String> subset = oNode.remove('subset')
        Map<String, Collection<Long>> otherRels = findOtherRelations(oNode)

        Long nodeId = node(id, labels[path], oNode, oLabels)

        createSynonyms(nodeId, synonyms)
        createXrefs(nodeId, xrefs)
        createIntersections(nodeId, intersections)
        createRelationships(nodeId, relationships)
        createSubsets(nodeId, subset)
        createOtherRels(nodeId, otherRels)

    }

    void createOtherRels(long nodeId, Map<String, Collection<Long>> rels) {
        rels.each { String relName, Collection<Long> relOntologyIds ->
            RelationshipType relType = DynamicRelationshipType.withName(relName.toUpperCase())
            for (Long relOntologyId in relOntologyIds) {
                linkToExternal(nodeId, relOntologyId, relType)
            }
        }
    }

    static Map<String, Collection<Long>> findOtherRelations(Map<String, ?> oNode) {
        oNode.findAll { String k, v -> v instanceof Collection }.each { oNode.remove(it.key) }
    }

    Collection<Label> getOntologyNodeLabels(namespace, subsets) {
        List labelStrings = [path, namespace]
        if (subsets) {
            for (String subset in subsets) {
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
        for (String rship in relationships) {
            createRelationshipFromString(rship, nodeId)
        }
    }

    // logical intersection, see http://geneontology.org/page/ontology-structure search for 'cross-products'
    def createIntersections(long nodeId, List intersections) {
        if (!intersections) return
        def (Long id, String name) = intersections
        linkToExternal(nodeId, id, Rels.INTERSECTION)
        createRelationshipFromString(name, nodeId)
    }


    private createRelationshipFromString(String rship, long nodeId) {
        if (rship ==~ ONTOLOGY_RELATIONSHIP_PATTERN) {
            def (_, String type, String oIdStr) = Matcher.lastMatcher[0]
            Long oId = Long.valueOf(oIdStr, 10)
            RelationshipType relType = DynamicRelationshipType.withName(type.toUpperCase())
            linkToExternal(nodeId, oId, relType)
        }
    }

    String namespace(String namespace) {
        if (namespace) {
            namespace = underscoreCaseToCamelCase(namespace)
            return labels[namespace]
        }
    }
}

@Singleton
class GOLoader extends OntologyLoader {
    @Override
    String getPath() { 'GO' }
}

@Singleton
class POLoader extends OntologyLoader {
    @Override
    String getPath() { 'PO' }
}

@Singleton
class TOLoader extends OntologyLoader {
    @Override
    String getPath() { 'TO' }
}

@Singleton
class EOLoader extends OntologyLoader {
    @Override
    String getPath() { 'EO' }
}

@Singleton
class SOLoader extends OntologyLoader {
    @Override
    String getPath() { 'SO' }
}

@Singleton
class GROLoader extends OntologyLoader {
    @Override
    String getPath() { 'GRO' }
}
