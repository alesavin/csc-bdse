package ru.csc.bdse.kv;

import ru.csc.bdse.model.KeyValueRecord;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class PartitionedKeyValueApiHttpClient {
    private List<KeyValueApi> nodes;

    public PartitionedKeyValueApiHttpClient(List<String> baseUrls) {
        nodes = new ArrayList<>();
        for (String baseUrl : baseUrls) {
            nodes.add(new KeyValueApiHttpClient(baseUrl));
        }
    }

    public String put(String key, byte[] value) {
        for (int i = 0; i < nodes.size(); ++i) {
            try {
                nodes.get(i).put(key, value);
                return "COMMIT";
            } catch (Exception e) {
                System.err.printf("Access error for node %d\n", i);
                e.printStackTrace();
            }
        }

        throw new IllegalStateException("None of nodes answered");
    }
    
    public Optional<KeyValueRecord> get(String key) {
        Optional<KeyValueRecord> record;
        for (int i = 0; i < nodes.size(); ++i) {
            try {
                record = nodes.get(i).get(key);
                if (record.isPresent()) {
                    return record;
                }
            } catch (Exception e) {
                System.err.printf("Access error for node %d\n", i);
                e.printStackTrace();
            }
        }

        throw new IllegalStateException("None of nodes answered");
    }

    public Set<String> getKeys(String prefix) {
        Set<String> allKeys = new HashSet<>();
        for (int i = 0; i < nodes.size(); ++i) {
            try {
                allKeys.addAll(nodes.get(i).getKeys(prefix));
            } catch (Exception e) {
                System.err.printf("Access error for node %d\n", i);
                e.printStackTrace();
            }
        }
        return allKeys;
    }

    public void delete(String key) {
        for (int i = 0; i < nodes.size(); ++i) {
            try {
                nodes.get(i).delete(key);
            } catch (Exception e) {
                System.err.printf("Access error for node %d\n", i);
                e.printStackTrace();
            }
        }
    }

    public void action(String node, NodeAction action) {
        for (int i = 0; i < nodes.size(); ++i) {
            try {
                nodes.get(i).action(node, action);
            } catch (Exception e) {
                System.err.printf("Access error for node %d\n", i);
                e.printStackTrace();
            }
        }
    }
}
