package ru.csc.bdse.kv;

import org.junit.*;
import org.springframework.web.client.HttpServerErrorException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.util.Env;

import java.io.File;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Test have to be implemented
 *
 * @author alesavin
 */
public class KeyValueApiHttpClientNonFunctionalTest {

    @ClassRule
    public static final GenericContainer node = new GenericContainer(
            new ImageFromDockerfile()
                    .withFileFromFile("target/bdse-kvnode-0.0.2-SNAPSHOT.jar", new File
                            ("../bdse-kvnode/target/bdse-kvnode-0.0.2-SNAPSHOT.jar"))
                    .withFileFromClasspath("Dockerfile", "kvnode/Dockerfile"))
            .withEnv(Env.KVNODE_NAME, "node-0")
            .withExposedPorts(8001)
            .withStartupTimeout(Duration.of(30, SECONDS));

    private KeyValueApi api = newKeyValueApi();

    private KeyValueApi newKeyValueApi() {
        final String baseUrl = "http://localhost:" + node.getMappedPort(8001);
        return new KeyValueApiHttpClient(baseUrl);
    }

    @Before
    public void initialize() {
        this.api.action("node-0", NodeAction.UP);

        for (String key : this.api.getKeys("")) {
            this.api.delete(key);
        }
    }

    @Test
    public void concurrentPuts() throws InterruptedException {
        Runnable putter = () -> {
            for (int i = 0; i < 100; ++i) {
                this.api.put(String.valueOf(i), Thread.currentThread().getName().getBytes());
            }
        };
        Thread[] threads = new Thread[10];
        for (int i = 0; i < 10; ++i) {
            threads[i] = new Thread(putter);
            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }
    }

    @Test
    public void concurrentDeleteAndKeys() throws InterruptedException{
        for (int i = 0; i < 1000; ++i) {
            this.api.put(String.valueOf(i), String.valueOf(i).getBytes());
        }
        Runnable lister = () -> {
            while (!this.api.getKeys("").isEmpty()) {
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
            }
        };

        Runnable deleter1 = () -> {
            for (int i = 0; i < 500; ++i) {
                this.api.delete(String.valueOf(i));
            }
        };

        Runnable deleter2 = () -> {
            for (int i = 500; i < 1000; ++i) {
                this.api.delete(String.valueOf(i));
            }
        };

        Thread listThread = new Thread(lister);
        Thread deleteFirstThread = new Thread(deleter1);
        Thread deleteSecondThread = new Thread(deleter2);

        listThread.start();
        deleteFirstThread.start();
        deleteSecondThread.start();

        listThread.join();
        deleteFirstThread.join();
        deleteSecondThread.join();
    }

    @Test
    public void actionUpDown() {
        NodeInfo ni = this.api.getInfo().iterator().next();
        Assert.assertEquals(ni.getStatus(), NodeStatus.UP);
        // Down node and check
        this.api.action(ni.getName(), NodeAction.DOWN);
        ni = this.api.getInfo().iterator().next();
        Assert.assertEquals(ni.getStatus(), NodeStatus.DOWN);
        // Up node and check
        this.api.action(ni.getName(), NodeAction.UP);
        ni = this.api.getInfo().iterator().next();
        Assert.assertEquals(ni.getStatus(), NodeStatus.UP);
    }

    @Test(expected = HttpServerErrorException.class)
    public void putWithStoppedNode() {
        api.action("node-0", NodeAction.DOWN);
        api.put("Foo", "bar".getBytes());
    }

    @Test(expected = HttpServerErrorException.class)
    public void getWithStoppedNode() {
        api.put("Foo", "bar".getBytes());
        api.action("node-0", NodeAction.DOWN);
        api.get("Foo");
    }

    @Test(expected = HttpServerErrorException.class)
    public void getKeysByPrefixWithStoppedNode() {
        api.action("node-0", NodeAction.DOWN);
        api.getKeys("F");
    }

    @Test
    public void deleteByTombstone() {
        // TODO use tombstones to mark as deleted (optional)
    }

    @Test
    public void loadMillionKeys()  {
        //TODO load too many data (optional)
    }
}


