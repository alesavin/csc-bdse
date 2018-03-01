package ru.csc.bdse.storage;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public interface Storage {
    int put(@NotNull String key, @NotNull byte[] data);

    @Nullable
    byte[] get(@NotNull String key);

    @Nullable
    byte[] get(@NotNull String key, int version, boolean includeDeleted);

    @NotNull
    Set<String> matchByPrefix(@NotNull String prefix, boolean includingDeleted);

    boolean delete(@NotNull String key);
}
