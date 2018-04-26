package ru.csc.bdse.kv;

import ru.csc.bdse.ApplicationProperties;
import ru.csc.bdse.datasource.BerkleyDataSource;

public class BerkleyKeyValueApiTest extends AbstractKeyValueApiTest {
    @Override
    protected KeyValueApi newKeyValueApi() {
        ApplicationProperties properties = new ApplicationProperties();
        properties.setDbfile("test.db");
        return new BerkleyKeyValueApi(new BerkleyDataSource(properties));
    }
}
