package graphene

import graphene.loaders.Loader
import groovy.util.logging.Log4j2
import org.junit.Test

import static org.junit.Assert.*
/**
 * Created by mulvaney on 11/5/14.
 */
@Log4j2
class LoaderTest {

    @Test
    void "load method is called"() {
        //Given
        Boolean iWasCalled = false
        Loader test = new Loader() {
            @Override
            protected void load() {
                iWasCalled = true
            }
        }
        assertFalse iWasCalled

        // When
        test.load(null, null, null)

        // Then
        assertTrue iWasCalled
    }
}
