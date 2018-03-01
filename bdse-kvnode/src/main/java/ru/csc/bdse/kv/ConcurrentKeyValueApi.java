package ru.csc.bdse.kv;

import org.jetbrains.annotations.NotNull;
import ru.csc.bdse.storage.Storage;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class ConcurrentKeyValueApi implements KeyValueApi {
    private final String name;
    private final ReadWriteLock lock = new ReentrantReadWriteLock(false);
    private final Storage storage;

    ConcurrentKeyValueApi(@NotNull String name, @NotNull Storage storage) {
        this.name = name;
        this.storage = storage;
    }

    @Override
    public void put(String key, byte[] data) {
        withWriteLock(() -> storage.put(key, data));
    }

    @Override
    public Optional<byte[]> get(String key) {
        return Optional.ofNullable(withReadLock(() -> storage.get(key)));
    }

    @Override
    public Set<String> getKeys(String prefix) {
        return withReadLock(() -> storage.matchByPrefix(prefix, false));
    }

    @Override
    public void delete(String key) {
        withWriteLock(() -> storage.delete(key));
    }

    @Override
    public Set<NodeInfo> getInfo() {
        return Collections.singleton(new NodeInfo(name, NodeStatus.UP));
    }

    @Override
    public void action(String node, NodeAction action) {
        throw new NotImplementedException();
    }

    private <T> T withReadLock(@NotNull Supplier<T> action) {
        try {
            lock.readLock().lock();
            return action.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void withWriteLock(@NotNull Runnable runnable) {
        try {
            lock.writeLock().lock();
            runnable.run();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
