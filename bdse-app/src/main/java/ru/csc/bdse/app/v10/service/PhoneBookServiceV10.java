package ru.csc.bdse.app.v10.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.csc.bdse.app.PhoneBookApi;
import ru.csc.bdse.app.RecordBookProtos;
import ru.csc.bdse.app.util.NameAndSurnameCannotContainAtException;
import ru.csc.bdse.app.v10.model.BookRecordV10;
import ru.csc.bdse.kv.BerkleyKeyValueApi;
import ru.csc.bdse.model.KeyValueRecord;
import ru.csc.bdse.util.Require;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

@Service
@Qualifier("PhoneBookServiceV10")
public class PhoneBookServiceV10 implements PhoneBookApi<BookRecordV10> {
    private final BerkleyKeyValueApi berkleyKeyValueApi;

    @Autowired
    public PhoneBookServiceV10(BerkleyKeyValueApi berkleyKeyValueApi) {
        this.berkleyKeyValueApi = berkleyKeyValueApi;
    }

    private String recordKey(BookRecordV10 record) {
        return record.getSecondName() + '@' + record.getFirstName();
    }

    @Override
    public void put(BookRecordV10 record) {
        Require.nonNull(record, "null key");

        String firstName = record.getFirstName();
        String secondName = record.getSecondName();
        String phone = record.getPhone();

        if (firstName.contains("@") || secondName.contains("@")) {
            throw new NameAndSurnameCannotContainAtException();
        }

        RecordBookProtos.Person.Builder person = RecordBookProtos.Person.newBuilder();
        person.setFirstName(firstName);
        person.setSecondName(secondName);
        person.addPhones(phone);

        byte[] byteRecord = person.build().toByteArray();
        berkleyKeyValueApi.put(recordKey(record), byteRecord);
    }

    @Override
    public void delete(BookRecordV10 record) {
        berkleyKeyValueApi.delete(recordKey(record));
    }

    @Override
    public Set<BookRecordV10> get(char literal) {
        Set<BookRecordV10> bookRecords = new HashSet<>();

        for (String key : berkleyKeyValueApi.getKeys(Character.toString(literal))) {
            Optional<KeyValueRecord> record = berkleyKeyValueApi.get(key);

            if (record.isPresent()) {
                byte[] bytes = record.get().getData();
                try {
                    RecordBookProtos.Person person = RecordBookProtos.Person.parseFrom(new ByteArrayInputStream(bytes));
                    BookRecordV10 recordV1 = new BookRecordV10(person.getFirstName(), person.getSecondName(), person.getPhones(0));
                    bookRecords.add(recordV1);
                } catch (IOException e) {
                    throw new IllegalArgumentException();
                }
            }
        }

        return bookRecords;
    }
}
