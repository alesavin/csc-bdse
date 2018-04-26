package ru.csc.bdse.app;

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;
import ru.csc.bdse.ApplicationProperties;
import ru.csc.bdse.app.v10.model.BookRecordV10;
import ru.csc.bdse.app.v10.service.PhoneBookServiceV10;
import ru.csc.bdse.datasource.BerkleyDataSource;
import ru.csc.bdse.kv.BerkleyKeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.util.IllegalNodeStateException;

import java.util.Set;

/**
 * Test have to be implemented
 *
 * @author alesavin
 */
public class PhoneBookNonFunctionalTest {

    private PhoneBookServiceV10 serviceV1;
    private BerkleyKeyValueApi berkleyKeyValueApi;

    @Before
    public void init() {
        ApplicationProperties properties = new ApplicationProperties();
        properties.setDbfile("test.db");

        berkleyKeyValueApi = new BerkleyKeyValueApi(new BerkleyDataSource(properties));
        berkleyKeyValueApi.setNodeName("testNode");
        berkleyKeyValueApi.action("testNode", NodeAction.DOWN);

        serviceV1 = new PhoneBookServiceV10(berkleyKeyValueApi);
    }

    @Test(expected = IllegalNodeStateException.class)
    public void putGetErasureWithStoppedNode() {
        SoftAssertions softAssert = new SoftAssertions();

        BookRecordV10 recordV1 = new BookRecordV10("Vasya", "Pupkin", "89115467734");
        serviceV1.put(recordV1);

        Set<BookRecordV10> bookRecordV1Set = serviceV1.get('P');
        softAssert.assertThat(!bookRecordV1Set.isEmpty());
    }

    @Test
    public void dataWasSavedIfAppRestarts() {
        // TODO test data was saved after app restarts
    }

    @Test
    public void dataWasSavedIfKvNodeRestarts() {
        SoftAssertions softAssert = new SoftAssertions();
        berkleyKeyValueApi.action("testNode", NodeAction.UP);

        BookRecordV10 recordV1 = new BookRecordV10("Vasya", "Pupkin", "89115467734");
        serviceV1.put(recordV1);

        ApplicationProperties properties = new ApplicationProperties();
        properties.setDbfile("test.db");
        berkleyKeyValueApi = new BerkleyKeyValueApi(new BerkleyDataSource(properties));

        serviceV1 = new PhoneBookServiceV10(berkleyKeyValueApi);

        Set<BookRecordV10> bookRecordV1Set = serviceV1.get('P');
        softAssert.assertThat(!bookRecordV1Set.isEmpty());
    }
}