package graphene

import org.neo4j.unsafe.batchinsert.BatchInserter

interface Loader {
    void load(BatchInserter batchInserter, NodeCache nodeCache, LabelCache labelCache)
}