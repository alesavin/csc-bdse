package ru.csc.bdse.resolver;

import ru.csc.bdse.model.KeyValueRecord;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface ConflictResolver {
    Optional<KeyValueRecord> resolve(List<KeyValueRecord> keyValueRecords);

    Set<String> resolveKeys(List<Set<String>> in);
}
