package ru.csc.bdse.datasource;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.csc.bdse.ApplicationProperties;

import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Service
public class BerkleyDataSource {

    private Environment dbEnvironment = null;
    private EntityStore store;

    @Autowired
    public BerkleyDataSource(ApplicationProperties applicationProperties) {
        String dbFilename = applicationProperties.getDbfile();
        try{
            if (Files.notExists(Paths.get(dbFilename))) {
                Files.createDirectories(Paths.get(dbFilename));
            }
        } catch (IOException e) {
            throw new RuntimeException("Directory " + dbFilename + " not exists and cannot be created.");
        }

        if (!Files.isDirectory(Paths.get(dbFilename))) {
            throw new RuntimeException(dbFilename + " should be directory");
        }

        try {
            EnvironmentConfig environmentConfig = new EnvironmentConfig();
            environmentConfig.setAllowCreate(true);
            dbEnvironment = new Environment(new File(dbFilename), environmentConfig);
            StoreConfig storeConfig = new StoreConfig();
            storeConfig.setAllowCreate(true);
            store = new EntityStore(dbEnvironment, "EntityStore", storeConfig);

        } catch (DatabaseException dbe) {
            System.out.println(dbe.toString());
        }
    }

    @PreDestroy
    private void destroy() {
        try {
            if (store != null) {
                store.close();
            }
            if (dbEnvironment != null) {
                dbEnvironment.close();
            }
        } catch (DatabaseException dbe) {
            System.out.println(dbe.toString());
        }

    }

    public EntityStore getStore() {
        return store;
    }

    public Environment getDbEnvironment() {
        return dbEnvironment;
    }
}
