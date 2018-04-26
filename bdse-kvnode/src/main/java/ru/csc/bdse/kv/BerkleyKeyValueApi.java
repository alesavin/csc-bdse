package ru.csc.bdse.kv;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.csc.bdse.datasource.BerkleyDataSource;
import ru.csc.bdse.model.KeyValueRecord;
import ru.csc.bdse.util.IllegalNodeStateException;
import ru.csc.bdse.util.Require;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

@Service
public class BerkleyKeyValueApi implements KeyValueApi {
    private final BerkleyDataSource berkleyDataSource;
    private NodeInfo nodeInfo;

    @Autowired
    public BerkleyKeyValueApi(BerkleyDataSource berkleyDataSource) {
        this.berkleyDataSource = berkleyDataSource;
        this.nodeInfo = new NodeInfo("UNKNOWN", NodeStatus.UP);
    }

    private PrimaryIndex<String, KeyValueRecord> getPrimaryIndex() {
        EntityStore store = berkleyDataSource.getStore();
        return store.getPrimaryIndex(String.class, KeyValueRecord.class);
    }

    /**
     * Puts value to the storage by specified key.
     */
    @Override
    public String put(String key, byte[] value) {
        Require.nonNull(key, "null key");
        Require.nonNull(value, "null value");

        this.checkNodeStatus();

        KeyValueRecord record = new KeyValueRecord(key, value);
        PrimaryIndex<String, KeyValueRecord> primaryIndex = getPrimaryIndex();
        primaryIndex.put(record);

        return "COMMIT";
    }

    /**
     * Returns value associated with specified key.
     */
    @Override
    public Optional<KeyValueRecord> get(String key) {
        Require.nonNull(key, "null key");

        this.checkNodeStatus();

        PrimaryIndex<String, KeyValueRecord> primaryIndex = getPrimaryIndex();
        KeyValueRecord record = primaryIndex.get(key);
        if (record == null) {
            return Optional.empty();
        }
        return Optional.of(record);
    }

    /**
     * Returns all keys with specified prefix.
     */
    @Override
    public Set<String> getKeys(String prefix) {
        this.checkNodeStatus();

        Set<String> keys = new HashSet<>();
        PrimaryIndex<String, KeyValueRecord> primaryIndex = getPrimaryIndex();
        try (EntityCursor<KeyValueRecord> entities = primaryIndex.entities()) {
            for (KeyValueRecord record : entities) {
                if (record.getKey().startsWith(prefix) && !record.isDeleted()) {
                    keys.add(record.getKey());
                }
            }
        }
        return keys;
    }

    /**
     * Deletes value associated with specified key from the storage.
     */
    @Override
    public void delete(String key) {
        Require.nonNull(key, "null key");

        this.checkNodeStatus();

        PrimaryIndex<String, KeyValueRecord> primaryIndex = getPrimaryIndex();
        KeyValueRecord record = primaryIndex.get(key);
        if (record == null) {
            return;
        }
        byte[] value = record.getData();

        KeyValueRecord deletedRecord = new KeyValueRecord(key, value, true);
        primaryIndex.put(deletedRecord);
    }

    /**
     * Returns info about all nodes.
     */
    @Override
    public Set<NodeInfo> getInfo() {
        return Collections.singleton(
                new NodeInfo(this.nodeInfo.getName(), this.nodeInfo.getStatus())
        );
    }

    public void setNodeName(String name) {
        this.nodeInfo = new NodeInfo(name, this.nodeInfo.getStatus());
    }

    private void checkNodeStatus() throws IllegalNodeStateException {
        if (this.nodeInfo.getStatus() == NodeStatus.DOWN) {
            throw new IllegalNodeStateException("Operational node is down.");
        }
    }

    /**
     * Do action on specified node.
     */
    @Override
    public void action(String node, NodeAction action) {
        if (!this.nodeInfo.getName().equals(node)) {
            throw new RuntimeException("Node with name " + node + " not found");
        }
        switch (action) {
            case UP:
                this.nodeInfo.setStatus(NodeStatus.UP);
                break;
            case DOWN:
                this.nodeInfo.setStatus(NodeStatus.DOWN);
                break;
            default:
                throw new RuntimeException("Unexpected node action");
        }
    }
}
