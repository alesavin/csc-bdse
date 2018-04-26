package ru.csc.bdse.resolver;

import ru.csc.bdse.model.KeyValueRecord;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;

public class Resolver implements ConflictResolver {
    @Override
    public Optional<KeyValueRecord> resolve(List<KeyValueRecord> allKeyValueRecords) {
        Set<KeyValueRecord> keyValueRecords = new HashSet<>(allKeyValueRecords);

        Optional<KeyValueRecord> maxKeyValueRecord = keyValueRecords.stream()
                .max(Comparator.comparing(KeyValueRecord::getTimestamp));

        if (maxKeyValueRecord.isPresent()) {
            long maxTime = maxKeyValueRecord.get().getTimestamp();
            Set<KeyValueRecord> elementsWithMaxTime = keyValueRecords.stream()
                    .filter(t -> t.getTimestamp() == maxTime)
                    .collect(Collectors.toSet());

            if (elementsWithMaxTime.size() > 1) {
                final Comparator<KeyValueRecord> comparator = Comparator
                        .comparingLong(KeyValueRecord::getTimestamp)
                        .thenComparing(KeyValueRecord::hashCode);

                maxKeyValueRecord = elementsWithMaxTime.stream().max(comparator);
            }

            if (!maxKeyValueRecord.orElseThrow(IllegalStateException::new).isDeleted()) {
                return maxKeyValueRecord;
            }
        }

        return Optional.empty();
    }


    @Override
    public Set<String> resolveKeys(List<Set<String>> setOfSetKeys) {
        Set<String> result = new HashSet<>();
        setOfSetKeys.forEach(result::addAll);

        return result;
    }
}
