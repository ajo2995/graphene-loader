package graphene.mongo;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import groovy.lang.Singleton;
import org.codehaus.groovy.runtime.DefaultGroovyMethods;

import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by mulvaney on 11/6/14.
 */
public class Mongo {

    private static final Mongo INSTANCE = new Mongo();
    public static Mongo get() {
        return INSTANCE;
    }
    private Mongo() {
        try {
            mongoClient = new MongoClient("brie.cshl.edu", 27017);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public DBCollection getCollection(String name) {
        DBCollection result = collections.get(name);

        if (!DefaultGroovyMethods.asBoolean(result)) {
            Map<String, String> config = MongoConfig.get().get(name);
            DB db = mongoClient.getDB(config.get("dbName"));
            result = db.getCollection(config.get("collectionName"));
            collections.put(name, result);
        }

        return result;
    }

    public static DBCollection get(String name) {
        return Mongo.get().getCollection(name);
    }

    public static void main(String[] args) {
        DBCollection genes = get("genes");

        System.out.println(genes.getCount());

//        DBCursor cursor = genes.find();

        int[] batchSizes = new int[]{0, 100, 1000, 10000, 100000};
        for(int i = 0; i < batchSizes.length; i++) {
            DBCursor cursor = genes.find();
            cursor.batchSize(batchSizes[i]);
            long start = System.currentTimeMillis();
            Integer count = 0;
            while (cursor.hasNext()) {
                cursor.next();
                if ((count = ++count) % 10_000 == 0) System.out.print(".");
            }

            System.out.println("\nBatch size " + batchSizes[i] + " finised in " + (System.currentTimeMillis() - start) + "ms");
        }

    }

    private MongoClient mongoClient;
    private final Map<String, DBCollection> collections = new LinkedHashMap<>();
}
