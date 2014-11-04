package graphene

class LoadGrameneGraphDb {

    final File store
    final Map config = [
            "use_memory_mapped_buffers": "true",
            "neostore.nodestore.db.mapped_memory": "250M",
            "neostore.relationshipstore.db.mapped_memory": "1G",
            "neostore.propertystore.db.mapped_memory": "500M",
            "neostore.propertystore.db.strings.mapped_memory": "500M",
            "neostore.propertystore.db.arrays.mapped_memory": "0M",
            "cache_type": "none",
            "dump_config": "true"
    ].asImmutable()

    public static void main(args) {
        String store
        if(args) {
            store = args.head()
        } else {
            store = File.createTempDir("tmpgraph",".db").absolutePath
//            store = "/usr/local/Cellar/neo4j/2.1.5/libexec/data/graph.db"
        }

        new LoadGrameneGraphDb(store)
    }

    public LoadGrameneGraphDb(String storePath) {
        store = new File(storePath)
        if(store.exists() && store.listFiles()) {
            throw new UnsupportedOperationException("Won't update an existing database: $store.canonicalPath")
        }
        if(!store.parentFile.canWrite()) {
            throw new FileNotFoundException("Can't write to $store.parentFile.canonicalPath")
        }
        println "Working with db directory $storePath"

        Importer i = new Importer(config, store)
    }
}
