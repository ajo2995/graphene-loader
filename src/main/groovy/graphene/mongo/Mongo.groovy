package graphene.mongo

import com.mongodb.DB
import com.mongodb.DBCollection
import com.mongodb.MongoClient
import org.codehaus.groovy.runtime.DefaultGroovyMethods

/**
 * Created by mulvaney on 11/6/14.
 */
@Singleton
public class Mongo {

    public DBCollection getCollection(String name) {
        DBCollection result = collections.get(name)

        if (!DefaultGroovyMethods.asBoolean(result)) {
            Map<String, String> config = MongoConfig.get().get(name)
            DB db = mongoClient.getDB(config.get("dbName"))
            result = db.getCollection(config.get("collectionName"))
            collections.put(name, result)
        }

        result
    }

    public static DBCollection get(String name) {
        return Mongo.instance.getCollection(name)
    }
    
    private MongoClient mongoClient = new MongoClient("brie.cshl.edu", 27017)
    private final Map<String, DBCollection> collections = [:]
}
