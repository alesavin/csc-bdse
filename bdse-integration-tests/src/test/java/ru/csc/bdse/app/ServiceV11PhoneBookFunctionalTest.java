package ru.csc.bdse.app;

import org.junit.Ignore;
import ru.csc.bdse.ApplicationProperties;
import ru.csc.bdse.app.v11.model.BookRecordV11;
import ru.csc.bdse.app.v11.service.PhoneBookServiceV11;
import ru.csc.bdse.datasource.BerkleyDataSource;
import ru.csc.bdse.kv.BerkleyKeyValueApi;
import ru.csc.bdse.util.Random;

import java.util.Arrays;

public class ServiceV11PhoneBookFunctionalTest extends AbstractPhoneBookFunctionalTest<BookRecordV11> {
    @Override
    protected PhoneBookApi<BookRecordV11> newPhoneBookApi() {
        ApplicationProperties properties = new ApplicationProperties();
        properties.setDbfile("test.db");
        return new PhoneBookServiceV11(new BerkleyKeyValueApi(new BerkleyDataSource(properties)));
    }

    @Override
    protected BookRecordV11 getNextRandomRecord() {
        return new BookRecordV11(Random.nextKey(), Random.nextKey(), Random.nextKey(), Arrays.asList(Random.nextKey(), Random.nextKey()));
    }

    @Override
    protected BookRecordV11 getNextRandomRecordWith(String firstName, String secondName) {
        return new BookRecordV11(firstName, secondName, Random.nextKey(), Arrays.asList(Random.nextKey(), Random.nextKey()));
    }
}
