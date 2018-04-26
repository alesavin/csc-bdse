package ru.csc.bdse.app;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import ru.csc.bdse.ApplicationProperties;
import ru.csc.bdse.app.v10.model.BookRecordV10;
import ru.csc.bdse.app.v10.service.PhoneBookServiceV10;
import ru.csc.bdse.app.v11.model.BookRecordV11;
import ru.csc.bdse.app.v11.service.PhoneBookServiceV11;
import ru.csc.bdse.datasource.BerkleyDataSource;
import ru.csc.bdse.kv.BerkleyKeyValueApi;

import java.util.List;
import java.util.Set;

/**
 * Test have to be implemented
 *
 * @author alesavin
 */
public class PhoneBookCompatibilitiesTest {

    private PhoneBookServiceV10 phoneBookServiceV10;
    private PhoneBookServiceV11 phoneBookServiceV11;

    @Before
    public void init() {
        ApplicationProperties properties = new ApplicationProperties();
        properties.setDbfile("test.db");
        phoneBookServiceV10 = new PhoneBookServiceV10(new BerkleyKeyValueApi(new BerkleyDataSource(properties)));
        phoneBookServiceV11 = new PhoneBookServiceV11(new BerkleyKeyValueApi(new BerkleyDataSource(properties)));
    }

    @Test
    public void write10read11() {
        SoftAssertions softAssert = new SoftAssertions();

        BookRecordV10 recordV1 = new BookRecordV10("Vasya", "Pupkin", "89115467734");
        phoneBookServiceV10.put(recordV1);

        Set<BookRecordV11> bookRecordV2Set = phoneBookServiceV11.get('P');
        softAssert.assertThat(!bookRecordV2Set.isEmpty());
    }

    @Test
    public void write11read10() {
        SoftAssertions softAssert = new SoftAssertions();
        List<String> phones = Lists.newArrayList("89115467735", "89115467738");
        BookRecordV11 recordV2 = new BookRecordV11("Vasya", "Pupkin", "v_pupkin", phones);
        phoneBookServiceV11.put(recordV2);

        Set<BookRecordV10> bookRecordV1Set = phoneBookServiceV10.get('P');
        softAssert.assertThat(!bookRecordV1Set.isEmpty());
    }

    @Test
    public void write10erasure11() {
        SoftAssertions softAssert = new SoftAssertions();
        BookRecordV10 recordV1 = new BookRecordV10("Vasya", "Pupkin", "89115467734");
        phoneBookServiceV10.put(recordV1);

        List<String> phones = Lists.newArrayList("89115467735", "89115467738");
        BookRecordV11 recordV2 = new BookRecordV11("Vasya", "Pupkin", "v_pupkin", phones);
        phoneBookServiceV11.delete(recordV2);

        Set<BookRecordV10> bookRecordV1Set = phoneBookServiceV10.get('P');
        softAssert.assertThat(bookRecordV1Set.isEmpty());
    }
}