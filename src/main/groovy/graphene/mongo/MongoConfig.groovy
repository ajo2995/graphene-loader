package graphene.mongo

import javax.script.ScriptEngine
import javax.script.ScriptEngineManager

/**
 * Created by mulvaney on 11/6/14.
 *
 * get javascript from // https://raw.githubusercontent.com/warelab/gramene-mongodb/master/config/collections.js
 * then get the collections information out.
 */
@Singleton(strict = false)
class MongoConfig {
    public static
    final String GRAMENE_MONGO_CONFIG_URL = 'https://raw.githubusercontent.com/warelab/gramene-mongodb/master/config/collections.js'
    final Map<String, Map> collections

    private MongoConfig() {
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn")
        engine.eval('var exports = {};') // the config JS puts useful info into exports, so let's provide an object
        engine.eval(GRAMENE_MONGO_CONFIG_URL.toURL().newReader())
        collections = engine.eval('exports;')?.collections.asImmutable()
    }

    static Map<String, Map<String, String>> get() {
        MongoConfig.instance.collections
    }

    static main(args) {
        println get()
    }
}
