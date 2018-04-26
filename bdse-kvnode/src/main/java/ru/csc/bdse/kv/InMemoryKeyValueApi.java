package ru.csc.bdse.kv;

import ru.csc.bdse.model.KeyValueRecord;
import ru.csc.bdse.util.Require;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Trivial in-memory implementation of the storage unit.
 *
 * @author semkagtn
 */
public class InMemoryKeyValueApi implements KeyValueApi {

    private final String name;
    private final ConcurrentMap<String, byte[]> map = new ConcurrentHashMap<>();

    public InMemoryKeyValueApi(final String name) {
        Require.nonEmpty(name, "empty name");
        this.name = name;
    }

    @Override
    public String put(final String key, final byte[] value) {
        Require.nonEmpty(key, "empty key");
        Require.nonNull(value, "null value");
        map.put(key, value);

        return "COMMIT";
    }

    @Override
    public Optional<KeyValueRecord> get(final String key) {
        Require.nonEmpty(key, "empty key");
        if (map.containsKey(key)) {
            return Optional.of(new KeyValueRecord(key, map.get(key)));
        }
        return Optional.empty();
    }

    @Override
    public Set<String> getKeys(String prefix) {
        Require.nonNull(prefix, "null prefix");
        return map.keySet()
                .stream()
                .filter(key -> key.startsWith(prefix))
                .collect(Collectors.toSet());
    }

    @Override
    public void delete(final String key) {
        Require.nonEmpty(key, "empty key");
        map.remove(key);
    }

    @Override
    public Set<NodeInfo> getInfo() {
        return Collections.singleton(new NodeInfo(name, NodeStatus.UP));
    }

    @Override
    public void action(String node, NodeAction action) {
        throw new RuntimeException("action not implemented now");
    }

}
