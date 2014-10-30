import org.neo4j.graphdb.DynamicLabel
import org.neo4j.graphdb.DynamicRelationshipType
import org.neo4j.graphdb.Label
import org.neo4j.unsafe.batchinsert.BatchInserter
import org.neo4j.unsafe.batchinsert.BatchInserters

@Grab('com.xlson.groovycsv:groovycsv:1.0')
@Grab('org.neo4j:neo4j:2.1.4')
import static com.xlson.groovycsv.CsvParser.parseCsv

// this script is derived from http://jexp.de/blog/2014/10/flexible-neo4j-batch-import-with-groovy/

// it assumes there is a folder called `db` with CSV exports of every table in the plant reactome. (I used Sequel Pro
// to do this on the mac)

// I installed neo4j using homebrew
String store = "/usr/local/Cellar/neo4j/2.1.5/libexec/data/graph.db"
Map config = [
        "use_memory_mapped_buffers": "true",
        "neostore.nodestore.db.mapped_memory": "250M",
        "neostore.relationshipstore.db.mapped_memory": "1G",
        "neostore.propertystore.db.mapped_memory": "500M",
        "neostore.propertystore.db.strings.mapped_memory": "500M",
        "neostore.propertystore.db.arrays.mapped_memory": "0M",
        "cache_type": "none",
        "dump_config": "true"
]

BatchInserter batch = BatchInserters.inserter(store, config)
File dir = new File("db")

// Keep track of all the labels that we create. This is important because we need to add indices to them at the end.
Map<String, Label> labelCache = [:].withDefault { String s -> DynamicLabel.label(s) }

// If we create new nodes
Map<Label, Map<String, Long>> newNodeCache = [:].withDefault { [:] }

// First add all the nodes from the DatabaseObject table export. These define most of the nodes in the graph, since
// it's a star schema.
loadNodesOrDie(batch, labelCache)

// then read the file again and add relationships. This is the lazy way to ensure that the nodes have been created before
// we try to add a relation to them.
loadOneToManyRelationshipsFromNodeTable(batch)

// Given a directory of csv files for every table in the reactome mysql database,
// 1. List them
Map<String, List<File>> files = getAndOrganizeFilesFrom(dir)

// OK let's deal with the "decorators". These define properties on nodes and relationships to other nodes.
// They also define a new "type" (in neo4j parlance, a `label`) that we will assign to the nodes.
processDecoratorData(files.decorators, labelCache, batch)

// So here we need to possibly create a new node if it hasn't already been created, and then add the
// relationship to the referenced node.
processFilesThatRequireNewNodes(files.newnodes, labelCache, newNodeCache, batch)

// Here we just need to create a relationship between two existing nodes
processFilesThatRelateNodes(files.relationships, batch)

// Index labels and add constraints
indexAndUniqueOnNamePropertyForAllLabels(labelCache.values(), batch)

batch.shutdown()

println "done."



private loadNodesOrDie(BatchInserter batch, Map<String, Label> labelCache) {
    if (!batch.nodeExists(1)) {
        def data = parseCsv(new File("db/DatabaseObject.csv").newReader())
        Long count = 0
        for (def line in data) {
            // we are
            Long id = Long.valueOf(line.DB_ID)
            Map props = [name: line._displayName]
            Label label = labelCache[line._class]
            batch.createNode(id, props, labelCache.Reactome, label)
            if (++count % 100_000 == 0) {
                println count
            }
        }

        println "$count nodes inserted"
    } else {
        println "Nodes already loaded. Should run this on an empty db."
        batch.shutdown()
        System.exit(1)
    }
}

private loadOneToManyRelationshipsFromNodeTable(BatchInserter batch) {
    if (true) {
        def data = parseCsv(new File("db/DatabaseObject.csv").newReader())
        Set<String> cols = data.columns.keySet()
        assert cols.contains('created')
        assert cols.contains('stableIdentifier')

        for (def line in data) {
            Long id = Long.valueOf(line.DB_ID)
            addRelationships(batch, id, line, ['created', 'stableIdentifier'])
        }
    }
}

private getAndOrganizeFilesFrom(File dir) {
    dir.listFiles()

// 2. Remove the special ones. (We've already dealt with DatabaseObject)
            .findAll { File f -> !['Ontology.csv', 'DatabaseObject.csv', 'DataModel.csv'].contains(f.name) && !(f.name =~ /^_/) }
// 3. Group them by type. There are three types:
            .groupBy { File f ->
        switch (f.name) {
//     a. Ones that will need new nodes to be created. These are m:m with a database object and just specify one property
            case ~/.*_2_name.*/:
            case ~/.*_2_synonym.*/:
            case ~/.*_2_ec.*/:
            case ~/.*_2_chain.*/:
            case ~/.*_2_otherIdentifier.*/:
            case ~/.*_2_secondCoordinate.*/:
            case ~/ReferenceSequence_2_.*/:
                "newnodes"
                break
//     b. Ones that define a m:m relationship between two existing types of node
            case ~/.*_2_.*/:
                "relationships"
                break
//     c. Ones that "decorate" a single node with additional properties and 1:m relationships to other types of node
            default:
                "decorators"
        }
    }
}

long getId(line) {
    Long.valueOf(line.DB_ID)
}

private processDecoratorData(List<File> decorators, Map<String, Label> labelCache, BatchInserter batch) {
    for (File f in decorators) {
        println f.name
        Label additionalLabel = labelCache[f.name[0..-5]]
        def data = parseCsv(f.newReader())
        Map cols = data.columns

        if (cols.size() == 0) {
            println "  $f.name has no columns; ignoring file"
            continue
        }

        List<String> rships = findRelationships(cols.keySet())
        List<String> props = findProps(cols.keySet(), rships)
        Integer lineNum = 1;
        for (def line in data) {
            ++lineNum;

            long id = getId(line)

            if (!id) {
                println "  No id on line $lineNum of $f.name; ignoring line"
                continue
            }

            // add new label
            addLabel(batch, id, additionalLabel)

            // add properties
            addProperties(batch, id, line, props)

            // add relationships
            addRelationships(batch, id, line, rships)
        }
        println "  processed $lineNum lines when processing decorators"
    }
}

private processFilesThatRequireNewNodes(List<File> newnodes, Map<String, Label> labelCache, Map<Label, Map<String, Long>> newNodeCache, BatchInserter batch) {
    for (File f in newnodes) {
        println f.name
        def data = parseCsv(f.newReader())
        def cols = data.columns.keySet()

        assert cols.size() == 3
        def props = findProps(cols)
        assert props.size() == 1
        String prop = props.head()
        assert cols.contains(prop + '_rank')

        Label label = labelCache[prop.capitalize()]

        Integer lineNum = 0
        for (def line in data) {
            ++lineNum
            long id = getId(line)

            if (!id) {
                println "No id on line $lineNum of $f.name; ignoring line"
                continue
            }

            String name = line[prop]

            Long newNodeId = newNodeCache[label][name]
            if (!newNodeId) {
                newNodeId = batch.createNode([name: name], labelCache.Reactome, label)
                newNodeCache[label][name] = newNodeId
            }

            String rshipName = camelCaseToConstantCase(prop)
            batch.createRelationship(id, newNodeId, DynamicRelationshipType.withName(rshipName), [rank: line[prop + '_rank']])
        }
        println "  processed $lineNum lines when creating new nodes"
    }
}

private processFilesThatRelateNodes(List<File> relationships, BatchInserter batch) {
    for (File f in relationships) {
        println f.name
        def data = parseCsv(f.newReader())
        def cols = data.columns
        assert cols.size() == 4

        def rships = findRelationships(cols.keySet())
        def props = findProps(cols.keySet(), rships)
        assert rships.size() == 1
        assert props.size() == 0

        Integer lineNum = 1;
        for (def line in data) {
            ++lineNum
            long id = getId(line)
            if (!id) {
                println "  No id on line $lineNum of $f.name; ignoring line"
                continue
            }

            addRelationships(batch, id, line, rships)
        }
        println "  processed $lineNum lines when adding relationships"
    }
}

private indexAndUniqueOnNamePropertyForAllLabels(Collection<Label> labels, BatchInserter batch) {
    for (Label l in labels) {
        println "Indexing ${l.name()} on 'name'"
        batch.createDeferredSchemaIndex(l).on("name").create();
    }
}


def addLabel(BatchInserter batch, long id, Label label) {
    Iterable<Label> currentLabels = batch.getNodeLabels(id)
    Set<Label> newLabels = new LinkedHashSet<Label>()
    for(Label l : currentLabels) newLabels.add(l)
    newLabels.add(label)
    batch.setNodeLabels(id, (Label[]) newLabels)
}

def addProperties(batch, id, line, List<String> props) {
    for(String prop in props) {
        if(line[prop] && line[prop] != 'NULL') {
            batch.setNodeProperty(id, prop, line[prop])
        }
    }
}

def addRelationships(batch, id, line, List<String> rships) {
    for(String rship in rships) {
        if(line[rship] && line[rship] != 'NULL') {
            long relation = Long.valueOf(line[rship])
            String neoRshipName = camelCaseToConstantCase(rship)
            Map relProps

            // m:m relationships have a _rank property to distinguish them, while m:1 don't
            if(line.columns[rship + '_rank'] && line[rship + '_rank']) {
                relProps = [ rank: Integer.valueOf(line[rship + '_rank']) ]
            }
            // convenient for m:1 to add the name of the related node as a property
            else {
                String relName = rship + 'Name'
                String relNameValue = batch.getNodeProperties(relation).name
                batch.setNodeProperty(id, relName, relNameValue)

                relProps = Collections.emptyMap()
            }
            batch.createRelationship(id, relation, DynamicRelationshipType.withName(neoRshipName), relProps)
        }
    }
}

def camelCaseToConstantCase(String camel) {
    // http://stackoverflow.com/a/3752693
    camel.split(/(?=\p{Upper})/).join('_').toUpperCase()
}

// convention seems to be that all relationships have two cols, one named <rship> and the other named <rship>_class.
// former contains referent DB_ID, latter contains class. We can look for "_class" props to identify relationships
def findRelationships(Set<String> colNames) {
    def rships = colNames.findAll{
        it =~ /_class$/
    }.collect{
        it[0..-7]
    }
    if(rships) println "  Found relationships $rships"
    rships
}

def findProps(Set<String> colNames, List<String> rships = Collections.emptyList()) {
    def result = new ArrayList(colNames) // explicit instantiate because we're using java remove* methods

    // remove relationship props and the special `DB_ID` one
    result.removeAll(rships) // this is a java operator that changes the source collections and returns a boolean. don't chain it!
    result.remove('DB_ID') // ditto

    // remove everything containing an `_` character
    result = result.findAll{ String col -> !(col.contains('_')) } // this is a groovy method that doesn't mutate the collection, so we need to reassign

    if(result) println "  Found props $result"
    result
}