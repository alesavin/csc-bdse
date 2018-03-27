package ru.csc.bdse.controller;

import feign.Feign;
import feign.RequestLine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;
import ru.csc.bdse.util.IllegalNodeStateException;

import java.util.*;
import java.util.concurrent.*;

/**
 * Provides HTTP API for the storage unit
 *
 * @author semkagtn
 */
@RestController
public class KeyValueApiController {
    private final KeyValueApi keyValueApi;

    @Value("${node.master}")
    private boolean isMaster;

    private List<String> slaves;

    private int port;

    @Value("${node.timeout}")
    private long timeout;

    private ExecutorService threadPool;

    private int numberOfOK;

    @Value("${node.WCL}")
    private int WCL;

    @Autowired
    public KeyValueApiController(final KeyValueApi keyValueApi,
                                 @Value("${bdse.nodes}") String nodesString,
                                 @Value("${server.port}") int port) {
        this.keyValueApi = keyValueApi;
        this.port = port;
        this.slaves = new ArrayList<>();
        this.threadPool = Executors.newFixedThreadPool(8);
        this.numberOfOK = 0;

        if (nodesString != null && isMaster) {
            String[] baseUrls = nodesString.split("(\\s|,)+");
            for (String baseUrl : baseUrls) {
                if (!baseUrl.split(":")[1].equals(String.valueOf(port))) {
                    slaves.add(baseUrl);
                }
            }
        }
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/key-value/{key}")
    public boolean put(@PathVariable final String key,
                       @RequestBody final byte[] value) throws InterruptedException, ExecutionException {


        Future<Boolean> masterFuture = CompletableFuture.supplyAsync(
                () -> {
                    boolean masterStatus = keyValueApi.put(key, value);

                    return masterStatus;
                },
                threadPool
        );

        boolean masterStatus;
        try {
            masterStatus = masterFuture.get(2, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            masterStatus = false;
        }

        if (masterStatus){
            numberOfOK += 1;
        }

        if (isMaster) {

            List<Future<Boolean>> futures = new ArrayList<>();

            for (String slave : slaves) {
                futures.add(
                        CompletableFuture.supplyAsync(
                                () -> {
                                    SlaveClient slaveClient = Feign.builder().target(SlaveClient.class, "http://" + slave);
                                    boolean slaveStatus = slaveClient.put(key, value);

                                    return slaveStatus;
                                },
                                threadPool
                        ));
            }

            for (Future<Boolean> future : futures) {
                boolean slaveStatus;
                try {
                    slaveStatus = future.get(2, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    slaveStatus = false;
                }

                if (slaveStatus){
                    numberOfOK += 1;
                }
            }

            threadPool.shutdown();
        }

        if (numberOfOK < WCL){
            throw new IllegalStateException("Error while recording");
        }

        return masterStatus;
    }

    interface SlaveClient {
        @RequestLine("/key-value/{key}")
        boolean put(@PathVariable final String key,
                    @RequestBody final byte[] value);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/key-value/{key}")
    public byte[] get(@PathVariable final String key) {
        return keyValueApi.get(key)
                .orElseThrow(() -> new NoSuchElementException(key));
    }

    @RequestMapping(method = RequestMethod.GET, value = "/key-value")
    public Set<String> getKeys(@RequestParam("prefix") String prefix) {
        return keyValueApi.getKeys(prefix);
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "/key-value/{key}")
    public void delete(@PathVariable final String key) {
        keyValueApi.delete(key);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/info")
    public Set<NodeInfo> getInfo() {
        return keyValueApi.getInfo();
    }

    @RequestMapping(method = RequestMethod.POST, value = "/action/{node}/{action}")
    public void action(@PathVariable final String node,
                       @PathVariable final NodeAction action) {
        keyValueApi.action(node, action);
    }

    @ExceptionHandler(NoSuchElementException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public String handle(NoSuchElementException e) {
        return Optional.ofNullable(e.getMessage()).orElse("");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public String handle(IllegalArgumentException e) {
        return Optional.ofNullable(e.getMessage()).orElse("");
    }

    @ExceptionHandler(IllegalNodeStateException.class)
    @ResponseStatus(value = HttpStatus.SERVICE_UNAVAILABLE, reason = "IllegalNodeState")
    public String handle(IllegalNodeStateException e) {
        return Optional.ofNullable(e.getMessage()).orElse("IllegalNodeState");
    }
}
