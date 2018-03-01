package ru.csc.bdse.kv;

import org.assertj.core.api.exception.RuntimeIOException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import ru.csc.bdse.storage.FileBasedStorage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

public class FileBasedStorageTest extends AbstractKeyValueApiTest {
    @Rule
    public final TemporaryFolder rule = new TemporaryFolder();
    private Path temporaryDirectory;

    @Before
    public void setUp() throws Exception {
        rule.create();
        temporaryDirectory = rule.newFolder().toPath();
        super.setUp();
    }

    @After
    public void tearDown() {
        rule.delete();
    }

    @Override
    protected KeyValueApi newKeyValueApi() {
        try {
            FileBasedStorage.createOrOpen(temporaryDirectory);
            return new ConcurrentKeyValueApi("testStorage", FileBasedStorage.createOrOpen(temporaryDirectory));
        } catch (IOException e) {
            System.err.println("Something went wrong: " + e);
            throw new UncheckedIOException("Cannot initialize/open database", e);
        }
    }
}
