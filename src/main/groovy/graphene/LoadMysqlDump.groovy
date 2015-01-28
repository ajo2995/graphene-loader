package graphene

import au.com.bytecode.opencsv.CSVParser
import com.xlson.groovycsv.PropertyMapper
import groovy.util.logging.Log4j2

import java.nio.file.Path
import java.nio.file.Paths

import static graphene.LoadMysqlDump.ParseState.*

@Log4j2
@Singleton
class LoadMysqlDump {

    public static final String NOT_DEFINED_IN_FILE = 'UNDEFINED'
    public static final String CHARSET = 'ISO-8859-1'

    public static void main(String[] args) {
        CliBuilder cli = new CliBuilder(usage: 'LoadGrameneGraphDB.groovy')
        cli.f(longOpt: 'file', args: 1, argName: 'store', required: true, 'location of dump')

        def opts = cli.parse(args)
        String dump = opts.f

        tablesFromDbDump(dump)
    }

    static Map<String, Table> tablesFromDbDump(String path) {
        Path p = Paths.get(path)
        TablesParser.parse(p)
    }

    static enum ParseState {
        BEFORE_CREATE(1), CREATE(2), COLUMN_DEFS(3), BEFORE_VALUES(4), VALUES(5), TABLE_DONE(6)
        Integer index

        private ParseState(i) {
            index = i
        }
    }

    @Singleton
    static class TablesParser {
        private final Map<String, Table> tables = [:]

        Path dbFile
        ParseState state

        TableBuilder tableBuilder

        static Map<String, Table> parse(Path dbFile) {
            TablesParser.instance.parseReader(dbFile)
        }

        private Map<String, Table> parseReader(Path dbFile) {
            this.dbFile = dbFile

            newTableBuilder()

            Integer lineNumber = 0
            dbFile.newReader(CHARSET).eachLine { String line ->
                updateState(line)
                processLine(line, lineNumber)
                ++lineNumber
            }

            return tables
        }

        def processLine(String line, Integer lineNumber) {
            // act on state and line
            switch (state) {
                case CREATE:
                    tableBuilder.tableDef = line
                    break
                case COLUMN_DEFS:
                    tableBuilder.columnDefs << line
                    break
                case VALUES:
                    tableBuilder.linesWithData << lineNumber
                    break
                case TABLE_DONE:
                    Table tableInfo = tableBuilder.done()
                    if(tableInfo.linesWithData) {
                        tables[tableInfo.name] = tableInfo
                    }
                    newTableBuilder()
                    break
            }
        }

        private void newTableBuilder() {
            state = BEFORE_CREATE
            tableBuilder = Table.build(dbFile)
        }

        def updateState(String line) {
            ParseState newState = state
            //noinspection GroovyFallthrough
            switch (state) {
                case BEFORE_CREATE:
                    if (line.startsWith('CREATE TABLE')) {
                        newState = CREATE
                    }
                    break
                case CREATE:
                    newState = COLUMN_DEFS // only one column def line
                    break
                case COLUMN_DEFS:
                    if (!line.startsWith('  `')) { // we are only looking for column defs and not PK, indices, etc
                        newState = BEFORE_VALUES
                    }
                    break
                case BEFORE_VALUES:
                    if (line.startsWith('INSERT INTO')) {
                        newState = VALUES
                    }
                    break
                case VALUES:
                    if (!line.startsWith('INSERT INTO')) {
                        newState = TABLE_DONE
                    }
                    break
                case TABLE_DONE:
                    newState = BEFORE_CREATE
                    break
            }

            // something untoward happened with this table: We reached the start of the next one but did not finish.
            // Let's bail on this one and then start again.
            if (state != BEFORE_CREATE && line.startsWith('-- Table structure for table `')) {
                newState = TABLE_DONE
                log.error "Got to end of table with no values"
            }

            assert newState == state ||
                    newState == TABLE_DONE ||
                    newState.index == state.index + 1 ||
                    (newState.index == 1 && state.index == ParseState.values().size())

            state = newState
        }


    }

    static class Table implements Iterable<PropertyMapper> {
        private static final CSVParser LINE_PARSER = new CSVParser((char)',', (char)'\'')

        String name
        Map<String, Integer> columns

        private List<Integer> linesWithData
        private Path dbFile

        static TableBuilder build(Path dbFile) {
            return new TableBuilder(dbFile: dbFile)
        }

        @Override
        Iterator<PropertyMapper> iterator() {
            return new Iterator<PropertyMapper>() {
                Iterator<List<String>> dataIterator = new DumpIterator(dbFile, linesWithData)

                @Override
                boolean hasNext() {
                    return dataIterator.hasNext()
                }

                @Override
                PropertyMapper next() {
                    String str = dataIterator.next()
                    return new PropertyMapper(columns: columns, values: LINE_PARSER.parseLine(str) as List )
                }
            }
        }
    }

    static class DumpIterator implements Iterator<String> {

        private final Reader dbFileReader
        private final Integer minLine
        private final Integer maxLine

        private Integer lineNumber = 0
        private Iterator<String> partialValues

        def DumpIterator(Path dbFile, List<Integer> lines) {
            minLine = Collections.min(lines)
            maxLine = Collections.max(lines)
            dbFileReader = dbFile.newReader(CHARSET)

            String line
            while(lineNumber < minLine) {
                // skip these lines
                lineNumber++
                dbFileReader.readLine()
            }

            populateValuesFromNextLine()
        }

        private populateValuesFromNextLine() {
            if(lineNumber > maxLine) {
                partialValues = Collections.emptyIterator()
                dbFileReader.close()
                return
            }

            String line = dbFileReader.readLine()

            List<String> values = line.split(/\),\(/)
            ++lineNumber

            try {
                if (values) {
                    values[0] = values[0].substring(values[0].indexOf('(') + 1)
                    values[-1] = values[-1][0..-3]
                }
            }
            catch(e) {
                log.error "Couldn't parse $values"
            }
            partialValues = values.iterator()
        }

        boolean moreLinesToRead() {
            lineNumber <= maxLine
        }

        @Override
        boolean hasNext() {
            return partialValues.hasNext() || moreLinesToRead()
        }

        @Override
        String next() {
            if(!partialValues.hasNext() && moreLinesToRead()) {
                populateValuesFromNextLine()
            }

            if(partialValues.hasNext()) {
                return partialValues.next()
            }

            throw new IndexOutOfBoundsException('End of iterator already reached')
        }
    }

    static class TableBuilder {
        Path dbFile
        String tableDef = NOT_DEFINED_IN_FILE
        List columnDefs = []
//        List values = []
        List linesWithData = []

        Table done() {
            String tableName = getTableName()
            if (!linesWithData) {
                log.error "$tableName has no data"
            }

            Map<String, Integer> columns = getColumns()

//            List<List<String>> data = getData(columns)

            return new Table(dbFile: dbFile, name: tableName, columns: columns, linesWithData: linesWithData)
        }

//        private List<List<String>> getData(Map<String, Integer> columns) {
//            final Integer numCols = columns.size()
//
//            List data = values.collect { String vals ->
//                List<String> things = vals.split(/\),\(/)
//                if (things) {
//                    things[0] = things[0].substring(things[0].indexOf('(') + 1)
//                    things[-1] = things[-1][0..-3]
//                }
//                return things
//            }.flatten().collect { String vals ->
//                LINE_PARSER.parseLine(vals) as List
//            }
//            return data
//        }

        private String getTableName() {
            String tableName = tableDef[14..-4]
            return tableName
        }

        private Map<String, Integer> getColumns() {
            List<String> columnNames = columnDefs.collect { it[it.indexOf('`') + 1..it.lastIndexOf('`') - 1] }
            Map<String, Integer> columns = columnNames.inject([:]) { Map map, String colName ->
                map[colName] = map.size()
                return map
            }
            return columns
        }
    }
}
