package ru.csc.bdse;

import org.junit.Test;
import ru.csc.bdse.model.KeyValueRecord;
import ru.csc.bdse.resolver.Resolver;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResolverTests {
    private Resolver resolver = new Resolver();

    private byte[] data = {4, 8, 15, 16, 23, 42};
    private KeyValueRecord simpleRecord = new KeyValueRecord("testKey", data);
    private Set<String> simpleKeySet = new HashSet<>(Arrays.asList("Marshall", "Fender", "Gibson"));


    @Test
    public void resolvePositive() {
        Optional<KeyValueRecord> result = resolver.resolve(
                Arrays.asList(simpleRecord, simpleRecord, simpleRecord)
        );
        assertTrue(result.isPresent());
        assertEquals(simpleRecord, result.get());
    }

    @Test
    public void resolveMaxTimestamp() {
        byte[] newData = {3, 1, 4, 1, 5, 9};
        KeyValueRecord newRecord = new KeyValueRecord("testKey", newData);
        newRecord.setTimestamp(System.currentTimeMillis() + 10);
        Optional<KeyValueRecord> result = resolver.resolve(
                Arrays.asList(simpleRecord, newRecord, simpleRecord)
        );
        assertTrue(result.isPresent());
        assertEquals(newRecord, result.get());
    }

    @Test
    public void resolveDeleted() {
        byte[] newData = {3, 1, 4, 1, 5, 9};
        KeyValueRecord newRecord = new KeyValueRecord("testKey", newData, true);
        newRecord.setTimestamp(System.currentTimeMillis() + 10);
        Optional<KeyValueRecord> result = resolver.resolve(
                Arrays.asList(simpleRecord, newRecord, simpleRecord)
        );
        assertFalse(result.isPresent());
    }

    @Test
    public void resolveKeysPositive() {
        Set<String> result = resolver.resolveKeys(
                Arrays.asList(simpleKeySet, simpleKeySet, simpleKeySet)
        );
        assertEquals(simpleKeySet, result);
    }

    @Test
    public void resolveKeysUnion() {
        Set<String> betterSet = new HashSet<>(simpleKeySet);
        betterSet.add("Yerasov");
        betterSet.remove("Marshall");

        Set<String> expected = new HashSet<>(Arrays.asList("Yerasov", "Marshall", "Fender", "Gibson"));

        Set<String> result = resolver.resolveKeys(
                Arrays.asList(simpleKeySet, betterSet, simpleKeySet)
        );
        assertEquals(expected, result);
    }
}
