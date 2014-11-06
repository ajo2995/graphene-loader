package graphene

import groovy.json.JsonSlurper
import groovy.util.logging.Log4j2
import org.neo4j.graphdb.Label

import java.util.regex.Matcher

import static graphene.Rels.SYNONYM
import static graphene.Rels.XREF

/**
 * Created by mulvaney on 10/31/14.
 */
@Log4j2
abstract class GrameneMongoLoader extends Loader {

    static final String URL_TEMPLATE = 'http://brie.cshl.edu:3000/%1$s/select?start=%2$d&rows=%3$d'
    static final Integer ROWS = 200

    static final JsonSlurper JSON_SLURPER = new JsonSlurper()

    abstract void process(Map result)

    abstract String getPath()

    private def parseJSON(Integer start) {
        URL url = createUrl(start)
        parseJSON(url)
    }

    static private def parseJSON(URL url) {
        JSON_SLURPER.parse(url)
    }

    private URL createUrl(Integer start) {
        sprintf(URL_TEMPLATE, getPath(), start, ROWS).toURL()
    }

    @Override
    void load() {
        Integer start = 0

        while (true) {
            def contents = parseJSON(start)
            for (Map taxon in contents.response) {
                preprocess(taxon)
                process(taxon)
            }
            start += ROWS
            if (start > contents.count) break
        }
    }

    static preprocess(Map entry) {
        entry.remove('_terms')
        entry.remove('alt_id')
        entry.remove('ancestors')
        entry.remove('namespace')
        entry.synonym = getSynonyms(entry.remove('synonym'))

        Matcher rankMatcher = entry.remove('property_value') =~ /has_rank NCBITaxon:(\w+)/
        if (rankMatcher) {
            entry.rank = ((List<String>) rankMatcher[0])[1]?.capitalize()
            // explicit cast to fail early if we didn't get any match groups
        }
    }


    static Set<String> getSynonyms(synonyms) {
        if (synonyms instanceof String) {
            synonyms = [synonyms]
        }
        synonyms as Set
    }

    void createSynonyms(Long nodeId, def synonyms) {
        // `synonyms` might be a scalar string or a list of strings

        Label nameLabel = labels.Name
        for (String s in synonyms) {
            long synonymNodeId = nodes.getOrCreate(nameLabel, s, batch)
            link(nodeId, synonymNodeId, SYNONYM)
        }
    }

    static String underscoreCaseToCamelCase(String s) {
        s?.toLowerCase()?.split('_')*.capitalize()?.join('')
    }

    def createXrefs(long nodeId, List<String> xrefs) {
        for (String xref in xrefs) {
            if (xref.indexOf(':') > 0) {
                def (String key, String value) = xref.split(':', 2)
                if (key != 'GC_ID') createXref(key, value, nodeId)
            }
        }
    }

    def createXref(String type, String name, Long referrerId) {
        Collection<Label> allLabels = labels.getLabels([type, 'Xref'])
        Map props = [name: name, type: type]

        if (['Reactome', 'VZ', 'http', 'loinc'].contains(type)) {
            String[] splitt = props.name.split(' ', 2)
            props.name = splitt[0]
            if (splitt.length > 1) props.desc = splitt[1]
        }

        Long xrefId = node(referrerId, labels[type], props, allLabels)
        link(referrerId, xrefId, XREF)
    }
}
