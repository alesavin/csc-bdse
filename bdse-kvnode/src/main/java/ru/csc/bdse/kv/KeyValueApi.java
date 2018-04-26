package ru.csc.bdse.kv;

import ru.csc.bdse.model.KeyValueRecord;

import java.util.Optional;
import java.util.Set;

/**
 * @author semkagtn
 */
public interface KeyValueApi {

    /**
     * Puts value to the storage by specified key.
     */
    String put(String key, byte[] value);

    /**
     * Returns value associated with specified key.
     */
    Optional<KeyValueRecord> get(String key);

    /**
     * Returns all keys with specified prefix.
     */
    Set<String> getKeys(String prefix);

    /**
     * Deletes value associated with specified key from the storage.
     */
    void delete(String key);

    /**
     * Returns info about all nodes.
     */
    Set<NodeInfo> getInfo();

    /**
     * Do action on specified node.
     */
    void action(String node, NodeAction action);
}
