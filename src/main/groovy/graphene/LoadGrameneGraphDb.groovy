package graphene

import groovy.util.logging.Log4j2

@Log4j2
class LoadGrameneGraphDb {

    final File store
    final File script
    final Map config = [
            "use_memory_mapped_buffers"                      : "true",
            "neostore.nodestore.db.mapped_memory"            : "250M",
            "neostore.relationshipstore.db.mapped_memory"    : "1G",
            "neostore.propertystore.db.mapped_memory"        : "500M",
            "neostore.propertystore.db.strings.mapped_memory": "500M",
            "neostore.propertystore.db.arrays.mapped_memory" : "0M",
            "cache_type"                                     : "none",
            "dump_config"                                    : "true"
    ].asImmutable()

    public static void main(args) {
        CliBuilder cli = new CliBuilder(usage: 'LoadGrameneGraphDB.groovy')
        cli.s(longOpt:'store', args:1, argName:'store', 'location of graph store')
        cli.c(longOpt:'cypher', args:1, argName:'script', 'location of cypher script to run after load')

        def opts = cli.parse(args)
        String store = opts.s ?: File.createTempDir("tmpgraph", ".db").absolutePath
        String script = opts.c ?: 'post-import-cypher.txt'

        new LoadGrameneGraphDb(store, script)
    }

    public LoadGrameneGraphDb(String storePath, String scriptPath) {
        store = new File(storePath)
        script = new File(scriptPath)
        if (store.exists() && store.listFiles()) {
            throw new UnsupportedOperationException("Won't update an existing database: $store.canonicalPath")
        }
        if (!store.parentFile.canWrite()) {
            throw new FileNotFoundException("Can't write to $store.parentFile.canonicalPath")
        }
        if(!script.canRead()) {
            throw new FileNotFoundException("Can't find file $script.canonicalPath")
        }
        log.info "Working with db directory $storePath and script $scriptPath"

        Importer.run(config, store)
    }

    private runPostLoadScript() {
        Process p = "neo4j-shell -file $script.canonicalPath".execute()
        p.inputStream.newReader().eachLine {
            log.info it
        }
        if(p.waitFor()) {
            log.error "something went wrong! Exit value $p.exitValue()"
            log.error p.errorStream.text
        }
    }
}
