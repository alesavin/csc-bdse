package ru.csc.bdse.kv;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.csc.bdse.ApplicationProperties;
import ru.csc.bdse.model.KeyValueRecord;
import ru.csc.bdse.partitioning.PartitionCoordinator;
import ru.csc.bdse.partitioning.Partitioner;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class PartitionedKeyValueApi implements KeyValueApi {

    private PartitionCoordinator coordinator;

    /**
     * @throws ClassNotFoundException - if partitioner from properties not found
     * @throws NoSuchMethodException - if partitioner hasn't constructor from Set<String>
     * @throws InvocationTargetException - if partitioner constructor throws exception
     * @throws InstantiationException - if partitioner cannot be instantiated
     * @throws IllegalAccessException - if partitioner constructor is private
     */
    @Autowired
    PartitionedKeyValueApi(ApplicationProperties applicationProperties) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        int timeout = applicationProperties.getNodeTimeout();

        List<KeyValueApi> partitionList = new ArrayList<>();
        for (String partitionUrl : applicationProperties.getPartitions()) {
            partitionList.add(new KeyValueApiHttpClient(partitionUrl));
        }
        Partitioner partitioner = applicationProperties.getPartitioner();

        this.coordinator = new PartitionCoordinator(partitionList, partitioner, timeout);
    }

    PartitionedKeyValueApi(List<KeyValueApi> partitionList, int timeout, Partitioner partitioner) {
        this.coordinator = new PartitionCoordinator(partitionList, partitioner, timeout);
    }

    @Override
    public String put(String key, byte[] value) {
        coordinator.put(key, value);
        return "COMMIT";
    }

    @Override
    public Optional<KeyValueRecord> get(String key) {
        return coordinator.get(key);
    }

    @Override
    public Set<String> getKeys(String prefix) {
        return coordinator.getKeys(prefix);
    }

    @Override
    public void delete(String key) {
        coordinator.delete(key);
    }

    @Override
    public Set<NodeInfo> getInfo() {
        return null;
    }

    @Override
    public void action(String node, NodeAction action) { }
}
