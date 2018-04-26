package ru.csc.bdse.partitioning;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeClient;
import ru.csc.bdse.model.KeyValueRecord;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PartitionCoordinator {
    private List<KeyValueApi> partitions;
    private int timeout;
    private Partitioner partitioner;
    private ExecutorService threadPool;

    public PartitionCoordinator(List<KeyValueApi> partitions, Partitioner partitioner, int timeout) {
        this.partitions = partitions;
        this.timeout = timeout;
        this.partitioner = partitioner;
        this.threadPool = Executors.newFixedThreadPool(8);
    }

    public void put(String key, byte[] value) {
        String nodeUrl = partitioner.getPartition(key);
        Future<String> future = CompletableFuture.supplyAsync(
                () -> {
                    NodeClient nodeClient = Feign.builder().target(NodeClient.class, nodeUrl);
                    return nodeClient.putInner(key, value);
                },
                threadPool
        );

        String nodeStatus;
        try {
            nodeStatus = future.get(this.timeout, TimeUnit.SECONDS);
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
            nodeStatus = "ABORT";
        }

        if (!nodeStatus.equals("COMMIT")) {
            throw new IllegalStateException("Time error while recording");
        }
    }

    public Optional<KeyValueRecord> get(String key) {
        String nodeUrl = partitioner.getPartition(key);
        Future<KeyValueRecord> future = CompletableFuture.supplyAsync(
                () -> {
                    NodeClient nodeClient = Feign.builder().decoder(new JacksonDecoder()).target(NodeClient.class, nodeUrl);
                    return nodeClient.getInner(key);
                },
                threadPool
        );

        KeyValueRecord record;
        try {
            record = future.get(this.timeout, TimeUnit.SECONDS);
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            if (e.getMessage().startsWith("feign.FeignException: status 404")) {
                record = new KeyValueRecord(key, null, true);
                record.setTimestamp(0);
            } else {
                record = null;
            }
        }

        return Optional.ofNullable(record);
    }

    public Set<String> getKeys(String prefix) {
        List<Future<Set<String>>> futures = new ArrayList<>();

        for (KeyValueApi api : partitions) {
            futures.add(
                    CompletableFuture.supplyAsync(
                            () -> api.getKeys(prefix),
                            threadPool
                    ));
        }

        Set<String> allKeys = new HashSet<>();

        for (Future<Set<String>> future : futures) {
            Set<String> keys;
            try {
                keys = future.get(this.timeout, TimeUnit.SECONDS);
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                keys = null;
            }

            if (keys != null) {
                allKeys.addAll(keys);
            }
        }

        return allKeys;
    }

    public void delete(String key) {
        String nodeUrl = partitioner.getPartition(key);
        Future<String> future = CompletableFuture.supplyAsync(
                () -> {
                    NodeClient nodeClient = Feign.builder().target(NodeClient.class, nodeUrl);
                    nodeClient.delete(key);
                    return "COMMIT";
                },
                threadPool
        );

        String nodeStatus;
        try {
            nodeStatus = future.get(this.timeout, TimeUnit.SECONDS);
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
            nodeStatus = "ABORT";
        }

        if (!nodeStatus.equals("COMMIT")) {
            throw new IllegalStateException("Time error while deleting");
        }
    }
}
