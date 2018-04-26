package ru.csc.bdse.app;

import org.junit.Ignore;
import ru.csc.bdse.ApplicationProperties;
import ru.csc.bdse.app.v10.model.BookRecordV10;
import ru.csc.bdse.app.v10.service.PhoneBookServiceV10;
import ru.csc.bdse.datasource.BerkleyDataSource;
import ru.csc.bdse.kv.BerkleyKeyValueApi;
import ru.csc.bdse.util.Random;

public class ServiceV10PhoneBookFunctionalTest extends AbstractPhoneBookFunctionalTest<BookRecordV10> {
    @Override
    protected PhoneBookApi<BookRecordV10> newPhoneBookApi() {
        ApplicationProperties properties = new ApplicationProperties();
        properties.setDbfile("test.db");
        return new PhoneBookServiceV10(new BerkleyKeyValueApi(new BerkleyDataSource(properties)));
    }

    @Override
    protected BookRecordV10 getNextRandomRecord() {
        return new BookRecordV10(Random.nextKey(), Random.nextKey(), Random.nextKey());
    }

    @Override
    protected BookRecordV10 getNextRandomRecordWith(String firstName, String secondName) {
        return new BookRecordV10(firstName, secondName, Random.nextKey());
    }
}
