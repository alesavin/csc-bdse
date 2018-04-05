package ru.csc.bdse.kv;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.csc.bdse.Application;
import ru.csc.bdse.ApplicationProperties;
import ru.csc.bdse.controller.PublicKeyValueApiController;
import ru.csc.bdse.model.KeyValueRecord;
import ru.csc.bdse.resolver.Resolver;

import java.util.*;
import java.util.concurrent.*;

@Service
public class ReplicatedKeyValueApi implements KeyValueApi{
    private ExecutorService threadPool;
    private String[] nodeUrls;
    private int timeout;
    private int WCL;
    private int RCL;

    @Autowired
    ReplicatedKeyValueApi(ApplicationProperties applicationProperties) {
        this.nodeUrls = applicationProperties.getNodes();
        this.timeout = applicationProperties.getNodeTimeout();
        this.WCL = applicationProperties.getNodeWCL();
        this.RCL = applicationProperties.getNoreRCL();
        this.threadPool = Executors.newFixedThreadPool(8);
    }

    /**
     * Puts value to the storage by specified key.
     *
     * @param key
     * @param value
     */
    @Override
    public String put(String key, byte[] value) {
        int numberOfOK = 0;

        List<Future<String>> futures = new ArrayList<>();

        for (String nodeUrl : nodeUrls) {
            futures.add(
                    CompletableFuture.supplyAsync(
                            () -> {
                                NodeClient nodeClient = Feign.builder().target(NodeClient.class, nodeUrl);
                                String nodeStatus = nodeClient.putInner(key, value);

                                return nodeStatus;
                            },
                            threadPool
                    ));
        }

        for (Future<String> future : futures) {
            String nodeStatus;
            try {
                nodeStatus = future.get(this.timeout, TimeUnit.SECONDS);
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                nodeStatus = "ABORT";
            }

            if (nodeStatus.equals("COMMIT")) {
                numberOfOK += 1;
            }
        }

        if (numberOfOK < WCL) {
            throw new IllegalStateException("Time error while recording");
        }

        return "COMMIT";
    }

    /**
     * Returns value associated with specified key.
     *
     * @param key
     */
    @Override
    public Optional<KeyValueRecord> get(String key) {
        int numberOfOK = 0;

        List<Future<KeyValueRecord>> futures = new ArrayList<>();

        for (String nodeUrl : nodeUrls) {
            futures.add(
                    CompletableFuture.supplyAsync(
                            () -> {
                                NodeClient nodeClient = Feign.builder().decoder(new JacksonDecoder()).target(NodeClient.class, nodeUrl);
                                KeyValueRecord record = nodeClient.getInner(key);

                                return record;
                            },
                            threadPool
                    ));
        }

        Set<KeyValueRecord> records = new HashSet<>();

        for (Future<KeyValueRecord> future : futures) {
            KeyValueRecord record;
            try {
                record = future.get(this.timeout * 5, TimeUnit.SECONDS);
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                record = null;
            }

            if (record != null) {
                numberOfOK += 1;
                records.add(record);
            }
        }

        if (numberOfOK < RCL) {
            throw new IllegalStateException("Time error while reading");
        }

        Resolver resolver = new Resolver();

        return resolver.resolve(records);
    }

    /**
     * Returns all keys with specified prefix.
     *
     * @param prefix
     */
    @Override
    public Set<String> getKeys(String prefix) {
        int numberOfOK = 0;

        List<Future<Set<String>>> futures = new ArrayList<>();

        for (String nodeUrl : nodeUrls) {
            futures.add(
                    CompletableFuture.supplyAsync(
                            () -> {
                                NodeClient nodeClient = Feign.builder().decoder(new JacksonDecoder()).target(NodeClient.class, nodeUrl);
                                Set<String> record = nodeClient.getsInner(prefix);

                                return record;
                            },
                            threadPool
                    ));
        }

        Set<Set<String>> records = new HashSet<>();

        for (Future<Set<String>> future : futures) {
            Set<String> record;
            try {
                record = future.get(this.timeout * 5, TimeUnit.SECONDS);
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                record = null;
            }

            if (record != null) {
                numberOfOK += 1;
                records.add(record);
            }
        }

        if (numberOfOK < RCL) {
            throw new IllegalStateException("Time error while reading");
        }

        Resolver resolver = new Resolver();

        return resolver.resolveKeys(records);
    }

    /**
     * Deletes value associated with specified key from the storage.
     *
     * @param key
     */
    @Override
    public void delete(String key) {

    }

    /**
     * Returns info about all nodes.
     */
    @Override
    public Set<NodeInfo> getInfo() {
        return null;
    }

    /**
     * Do action on specified node.
     *
     * @param node
     * @param action
     */
    @Override
    public void action(String node, NodeAction action) {

    }
}
