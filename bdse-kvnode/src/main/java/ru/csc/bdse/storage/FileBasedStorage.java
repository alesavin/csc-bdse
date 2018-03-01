package ru.csc.bdse.storage;

import org.apache.commons.io.input.CountingInputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class FileBasedStorage implements Storage {
    private static final String KEYS_FILENAME = "keys.argo";
    private static final String DATA_FILENAME = "data.argo";
    private final File keysFile;
    private final File dataFile;

    private FileBasedStorage(@NotNull File keys, @NotNull File data) {
        this.keysFile = keys;
        this.dataFile = data;
    }

    public static Storage createOrOpen(@NotNull Path directory) throws IOException {
        Storage existed = tryOpenExisted(directory);
        if (existed != null) {
            return existed;
        }

        return initNewDatabase(directory);
    }

    @Override
    public int put(@NotNull String key, @NotNull byte[] data) {
        IndexFileRecord record = getIndexRecord(key);
        if (record != null) {
            DataFileRecord dataRecord = findDataRecord(record.offset);
            long newDataOffset = storeData(data, record.dataOffset, dataRecord.version);
            updateKeyRecord(record.offset, newDataOffset, false);
        } else {
            long newDataOffset = storeData(data, -1, 1);
            createKeyRecord(key, newDataOffset);
            return 0;
        }
        return 0;
    }

    @Nullable
    @Override
    public byte[] get(@NotNull String key) {
        IndexFileRecord record = getIndexRecord(key);
        if (record == null || record.isDeleted) {
            return null;
        }

        return readData(record.dataOffset);
    }

    @Nullable
    @Override
    public byte[] get(@NotNull String key, int version, boolean includeDeleted) {
        if (version < 1) {
            throw new IllegalArgumentException("Version must be positive integer");
        }

        IndexFileRecord record = getIndexRecord(key);
        if (record == null || (!includeDeleted && record.isDeleted)) {
            return null;
        }

        DataFileRecord dataRecord = findDataRecord(record.offset);
        if (dataRecord.version < version) {
            throw new IllegalArgumentException("Version must be less than the newest version");
        }

        while (version < dataRecord.version) {
            dataRecord = findDataRecord(dataRecord.previousVersionOffset);
        }

        if (version != dataRecord.version) {
            throw new IllegalStateException("Version \"" + version + "\" not found");
        }

        return readData(dataRecord.offset);
    }

    @NotNull
    @Override
    public Set<String> matchByPrefix(@NotNull String prefix, boolean includeDeleted) {
        return scanKeys(prefix, RetrievePolicy.MATCH_PREFIX, false).stream()
                .map(x -> x.key)
                .collect(Collectors.toSet());
    }

    @Override
    public boolean delete(@NotNull String key) {
        IndexFileRecord record = getIndexRecord(key);
        if (record == null || record.isDeleted) {
            return false;
        }

        delete(record);
        return true;
    }

    @Nullable
    private IndexFileRecord getIndexRecord(@NotNull String key) {
        Set<IndexFileRecord> result = scanKeys(key, RetrievePolicy.MATCH_FIRST, true);
        return result.stream().findAny().orElse(null);
    }

    @NotNull
    private Set<IndexFileRecord> scanKeys(@NotNull String key, @NotNull RetrievePolicy retrievePolicy,
                                          boolean includingDeleted) {
        Set<IndexFileRecord> result = new HashSet<>();
        try (CountingInputStream countingStream = open(keysFile)) {
            DataInputStream dis = new DataInputStream(countingStream);
            while (dis.available() > 0) {
                long offset = countingStream.getByteCount();
                long dataOffset = dis.readLong();
                boolean isDeleted = dis.readBoolean();
                String recordKey = dis.readUTF();
                if (!includingDeleted && isDeleted) continue;
                if (RetrievePolicy.MATCH_FIRST.equals(retrievePolicy) && key.equals(recordKey)) {
                    result.add(new IndexFileRecord(recordKey, offset, dataOffset, isDeleted));
                    break;
                }
                if (RetrievePolicy.MATCH_PREFIX.equals(retrievePolicy) && recordKey.startsWith(key)) {
                    result.add(new IndexFileRecord(recordKey, offset, dataOffset, isDeleted));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return result;
    }

    private void delete(IndexFileRecord record) {
        updateKeyRecord(record.offset, record.dataOffset, true);
    }

    private void createKeyRecord(@NotNull String key, long newDataOffset) {
        try (RandomAccessFile file = new RandomAccessFile(keysFile, "rw")) {
            long offset = file.length();
            file.seek(offset);
            file.writeLong(newDataOffset);
            file.writeBoolean(false);
            file.writeUTF(key);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void updateKeyRecord(long offset, long dataOffset, boolean isDeleted) {
        try (RandomAccessFile file = new RandomAccessFile(keysFile, "rw")) {
            file.seek(offset);
            file.writeLong(dataOffset);
            file.writeBoolean(isDeleted);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @NotNull
    private DataFileRecord findDataRecord(long offset) {
        try (RandomAccessFile file = new RandomAccessFile(dataFile, "r")) {
            file.seek(offset);
            long prevVersionOffset = file.readLong();
            int length = file.readInt();
            int version = file.readInt();
            return new DataFileRecord(prevVersionOffset, length, version, offset);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @NotNull
    private byte[] readData(long offset) {
        try (RandomAccessFile file = new RandomAccessFile(dataFile, "r")) {
            file.seek(offset);
            file.readLong(); // previous version offset
            int length = file.readInt();
            file.readInt(); // version

            byte[] result = new byte[length];
            file.readFully(result);
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private long storeData(@NotNull byte[] data, long previousVersionOffset, int version) {
        try (RandomAccessFile file = new RandomAccessFile(dataFile, "rw")) {
            long offset = file.length();
            file.setLength(offset + DataFileRecord.SIZE + data.length);
            file.seek(offset);
            file.writeLong(previousVersionOffset);
            file.writeInt(data.length);
            file.writeInt(version);
            file.write(data);
            return offset;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static CountingInputStream open(@NotNull File file) throws FileNotFoundException {
        return new CountingInputStream(new BufferedInputStream(new FileInputStream(file)));
    }

    @NotNull
    private static Storage initNewDatabase(@NotNull Path directory) throws IOException {
        if (!directory.toFile().exists()) {
            if (!directory.toFile().mkdirs()) {
                throw new IOException("unable to create a directory to store a database");
            }
        }


        File keysFile = directory.resolve(KEYS_FILENAME).toFile();
        if (!keysFile.createNewFile()) {
            throw new IOException("unable to create a file to store keys");
        }

        File dataFile = directory.resolve(DATA_FILENAME).toFile();
        if (!dataFile.createNewFile()) {
            throw new IOException("unable to create a file to store data");
        }

        return new FileBasedStorage(keysFile, dataFile);
    }

    @Nullable
    private static Storage tryOpenExisted(@NotNull Path directory) throws IOException {
        if (!directory.toFile().exists()) {
            return null;
        }

        boolean keysExist = directory.resolve(KEYS_FILENAME).toFile().exists();
        boolean dataExist = directory.resolve(DATA_FILENAME).toFile().exists();

        if (keysExist && !dataExist) {
            throw new IOException("unable to open database: data file not found");
        }
        if (!keysExist && dataExist) {
            throw new IOException("unable to open database: file with keys not found");
        }

        if (keysExist) {
            return new FileBasedStorage(directory.resolve(KEYS_FILENAME).toFile(), directory.resolve(DATA_FILENAME).toFile());
        }

        return null;
    }

    private enum RetrievePolicy {
        MATCH_PREFIX, MATCH_FIRST
    }

    private static class IndexFileRecord {
        final long dataOffset;
        final boolean isDeleted;
        final String key;

        final long offset;

        private IndexFileRecord(String key, long offset, long dataOffset, boolean isDeleted) {
            this.key = key;
            this.offset = offset;
            this.dataOffset = dataOffset;
            this.isDeleted = isDeleted;
        }
    }

    private static class DataFileRecord {
        static int SIZE = 8 + 4 + 4;

        final long previousVersionOffset;
        final int length;
        final int version;

        final long offset;

        private DataFileRecord(long previousVersionOffset, int length, int version, long offset) {
            this.previousVersionOffset = previousVersionOffset;
            this.length = length;
            this.version = version;
            this.offset = offset;
        }
    }
}
