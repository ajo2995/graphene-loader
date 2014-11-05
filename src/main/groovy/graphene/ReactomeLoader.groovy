package graphene

import groovy.util.logging.Log4j2
import org.neo4j.graphdb.DynamicRelationshipType
import org.neo4j.graphdb.Label
import org.neo4j.unsafe.batchinsert.BatchInserter

import static com.xlson.groovycsv.CsvParser.parseCsv

@Log4j2
class ReactomeLoader extends Loader {

    final Map<String, List<File>> files

    ReactomeLoader(String location = "db") {
        File dir = new File(location)
        if (!dir.exists() || !dir.canRead()) {
            throw new FileNotFoundException("Either can't find or read from $dir.canonicalPath")
        }

        // Given a directory of csv files for every table in the reactome mysql database,
// 1. List them
        files = getAndOrganizeFilesFrom(dir)
    }

    @Override
    void load() {
// First add all the nodes from the DatabaseObject table export. These define most of the nodes in the graph, since
// it's a star schema.
        loadNodesOrDie()

// then read the file again and add relationships. This is the lazy way to ensure that the nodes have been created before
// we try to add a relation to them.
        loadOneToManyRelationshipsFromNodeTable()

// OK let's deal with the "decorators". These define properties on nodes and relationships to other nodes.
// They also define a new "type" (in neo4j parlance, a `label`) that we will assign to the nodes.
        processDecoratorData(files.decorators)

// So here we need to possibly create a new node if it hasn't already been created, and then add the
// relationship to the referenced node.
        processFilesThatRequireNewNodes(files.newnodes)

// Here we just need to create a relationship between two existing nodes
        processFilesThatRelateNodes(files.relationships)

    }

    private loadNodesOrDie() {
        if (!batch.nodeExists(1)) {
            def data = parseCsv(new File("db/DatabaseObject.csv").newReader())
            Long count = 0
            for (def line in data) {
                // we are
                Long id = getId(line)
                Map props = [name: line._displayName]
                Label label = labels[line._class]
                nodeWithId(id, props, label, labels.Reactome)
                if (++count % 100_000 == 0) {
                    log.info count
                }
            }

            log.info "$count nodes inserted"
        } else {
            log.info "Nodes already loaded. Should run this on an empty db."
            batch.shutdown()
            System.exit(1)
        }
    }

    private loadOneToManyRelationshipsFromNodeTable() {
        def data = parseCsv(new File("db/DatabaseObject.csv").newReader())
        Set<String> cols = data.columns.keySet()
        assert cols.contains('created')
        assert cols.contains('stableIdentifier')

        for (def line in data) {
            Long id = getId(line)
            addRelationships(id, line, ['created', 'stableIdentifier'])
        }
    }

    static private getAndOrganizeFilesFrom(File dir) {
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

    static long getId(line) {
        Long.valueOf((String)line.DB_ID)
    }

    private processDecoratorData(List<File> decorators) {
        for (File f in decorators) {
            log.info f.name
            Label additionalLabel = labels[f.name[0..-5]]
            def data = parseCsv(f.newReader())
            Map cols = data.columns

            if (cols.size() == 0) {
                log.info "  $f.name has no columns; ignoring file"
                continue
            }

            List<String> rships = findRelationships(cols.keySet())
            List<String> props = findProps(cols.keySet(), rships)
            Integer lineNum = 1;
            for (def line in data) {
                ++lineNum;

                long id = getId(line)

                if (!id) {
                    log.info "  No id on line $lineNum of $f.name; ignoring line"
                    continue
                }

                // add new label
                addLabel(id, additionalLabel)

                // add properties
                addProperties(id, line, props)

                // add relationships
                addRelationships(id, line, rships)
            }
            log.info "  processed $lineNum lines when processing decorators"
        }
    }


    private processFilesThatRequireNewNodes(List<File> newnodes) {
        for (File f in newnodes) {
            log.info f.name
            def data = parseCsv(f.newReader())
            def cols = data.columns.keySet()

            assert cols.size() == 3
            def props = findProps(cols)
            assert props.size() == 1
            String prop = props.head()
            assert cols.contains(prop + '_rank')

            Label label = labels[prop.capitalize()]

            Integer lineNum = 0
            for (def line in data) {
                ++lineNum
                long id = getId(line)

                if (!id) {
                    log.error "No id on line $lineNum of $f.name; $line; ignoring line"
                    continue
                }

                String name = line[prop]
                if(!name) {
                    log.error("Name is empty for $lineNum of $f.name; $line; ignoring line")
                    continue
                }

                Long newNodeId = node(label, [name: name], [labels.Reactome])

                String rshipName = camelCaseToConstantCase(prop)
                link(id, newNodeId, DynamicRelationshipType.withName(rshipName), [rank: line[prop + '_rank']])
            }
            log.info "  processed $lineNum lines when creating new nodes"
        }
    }

    private processFilesThatRelateNodes(List<File> relationships) {
        for (File f in relationships) {
            log.info f.name
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
                    log.info "  No id on line $lineNum of $f.name; ignoring line"
                    continue
                }

                addRelationships(id, line, rships)
            }
            log.info "  processed $lineNum lines when adding relationships"
        }
    }

    def addLabel(long id, Label label) {
        Iterable<Label> currentLabels = batch.getNodeLabels(id)
        Set<Label> newLabels = new LinkedHashSet<Label>()
        for (Label l : currentLabels) newLabels.add(l)
        newLabels.add(label)
        batch.setNodeLabels(id, (Label[]) newLabels)
    }

    def addProperties(long id, line, List<String> props) {
        for (String prop in props) {
            if (line[prop] && line[prop] != 'NULL') {
                batch.setNodeProperty(id, prop, line[prop])
            }
        }
    }

    def addRelationships(long id, line, List<String> rships) {
        for (String rship in rships) {
            if (line[rship] && line[rship] != 'NULL') {
                long relation = Long.valueOf(line[rship])
                String neoRshipName = camelCaseToConstantCase(rship)
                Map relProps

                // m:m relationships have a _rank property to distinguish them, while m:1 don't
                if (line.columns[rship + '_rank'] && line[rship + '_rank']) {
                    relProps = [rank: Integer.valueOf(line[rship + '_rank'])]
                }
                // convenient for m:1 to add the name of the related node as a property
                else {
                    String relName = rship + 'Name'
                    String relNameValue = batch.getNodeProperties(relation).name
                    batch.setNodeProperty(id, relName, relNameValue)

                    relProps = Collections.emptyMap()
                }
                link(id, relation, DynamicRelationshipType.withName(neoRshipName), relProps)
            }
        }
    }

    static def camelCaseToConstantCase(String camel) {
        // http://stackoverflow.com/a/3752693
        camel.split(/(?=\p{Upper})/).join('_').toUpperCase()
    }

// convention seems to be that all relationships have two cols, one named <rship> and the other named <rship>_class.
// former contains referent DB_ID, latter contains class. We can look for "_class" props to identify relationships
    def findRelationships(Set<String> colNames) {
        def rships = colNames.findAll {
            it =~ /_class$/
        }.collect {
            it[0..-7]
        }
        if (rships) log.info "  Found relationships $rships"
        rships
    }

    List<String> findProps(Set<String> colNames, List<String> rships = Collections.emptyList()) {
        List<String> result = new ArrayList(colNames) // explicit instantiate because we're using java remove* methods

        // remove relationship props and the special `DB_ID` one
        result.removeAll(rships)
        // this is a java operator that changes the source collections and returns a boolean. don't chain it!
        result.remove('DB_ID') // ditto

        // remove everything containing an `_` character
        result = result.findAll { String col -> !(col.contains('_')) }
        // this is a groovy method that doesn't mutate the collection, so we need to reassign

        if (result) log.info "  Found props $result"
        result
    }
}