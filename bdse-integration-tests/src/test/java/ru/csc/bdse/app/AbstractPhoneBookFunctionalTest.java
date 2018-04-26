package ru.csc.bdse.app;

import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.Test;
import ru.csc.bdse.app.util.NameAndSurnameCannotContainAtException;
import ru.csc.bdse.util.Random;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractPhoneBookFunctionalTest<T extends Record> {

    protected abstract PhoneBookApi<T> newPhoneBookApi();
    private PhoneBookApi<T> api = newPhoneBookApi();
    private Set<Character> addedLiterals = new HashSet<>();

    protected abstract T getNextRandomRecord();
    protected abstract T getNextRandomRecordWith(String firstName, String secondName);

    @After
    public void clean() {
        for (char literal: this.addedLiterals) {
            for (T record: this.api.get(literal)) {
                this.api.delete(record);
            }
        }
        this.addedLiterals.clear();
    }

    private void addRecord(T record) {
        this.api.put(record);
        this.addedLiterals.addAll(record.literals());
    }

    // Gets records from an empty phone book.
    @Test
    public void getFromEmptyBook() {
        SoftAssertions softAssert = new SoftAssertions();
        softAssert.assertThat(api.get(Random.nextChar())).isEmpty();

        softAssert.assertAll();
    }

    // Puts and gets some test records.
    @Test
    public void putAndGet() {
        SoftAssertions softAssert = new SoftAssertions();

        T firstRecord = getNextRandomRecord();
        T secondRecord = getNextRandomRecord();

        addRecord(firstRecord);

        for (char literal: firstRecord.literals()) {
            softAssert.assertThat(api.get(literal).contains(firstRecord)).isTrue();
        }
        for (char literal: secondRecord.literals()) {
            softAssert.assertThat(api.get(literal).contains(secondRecord)).isFalse();
        }

        addRecord(secondRecord);
        for (char literal: firstRecord.literals()) {
            softAssert.assertThat(api.get(literal).contains(firstRecord)).isTrue();
        }
        for (char literal: secondRecord.literals()) {
            softAssert.assertThat(api.get(literal).contains(secondRecord)).isTrue();
        }

        softAssert.assertAll();
    }

    // Puts and erases a test record.
    @Test
    public void erasure() {
        SoftAssertions softAssert = new SoftAssertions();

        T record = getNextRandomRecord();
        for (char literal: record.literals()) {
            softAssert.assertThat(api.get(literal).contains(record)).isFalse();
        }

        addRecord(record);
        for (char literal: record.literals()) {
            softAssert.assertThat(api.get(literal).contains(record)).isTrue();
        }

        api.delete(record);
        for (char literal: record.literals()) {
            softAssert.assertThat(api.get(literal).contains(record)).isFalse();
        }

        softAssert.assertAll();
    }

    // Updates data.
    @Test
    public void update() {
        SoftAssertions softAssert = new SoftAssertions();

        String firstName = Random.nextKey();
        String secondName = Random.nextKey();

        T record = getNextRandomRecordWith(firstName, secondName);
        addRecord(record);
        for (char literal: record.literals()) {
            softAssert.assertThat(api.get(literal).contains(record)).isTrue();
        }

        T newRecord = getNextRandomRecordWith(firstName, secondName);
        addRecord(newRecord);
        for (char literal: newRecord.literals()) {
            softAssert.assertThat(api.get(literal).contains(record)).isFalse();
            softAssert.assertThat(api.get(literal).contains(newRecord)).isTrue();
        }

        softAssert.assertAll();
    }

    // Adds some data twice.
    @Test
    public void twicePutAndGet() {
        SoftAssertions softAssert = new SoftAssertions();

        T record = getNextRandomRecord();
        addRecord(record);
        addRecord(record);

        for (char literal: record.literals()) {
            softAssert.assertThat(api.get(literal)).hasSize(1);
        }

        softAssert.assertAll();
    }

    // Checks correctness of work with data with same surnames but different first names.
    @Test
    public void sameSurnamesDifferentNames() {
        SoftAssertions softAssert = new SoftAssertions();

        String secondName = Random.nextKey();

        String firstName1 = Random.nextKey();
        String firstName2 = Random.nextKey();

        T record1 = getNextRandomRecordWith(firstName1, secondName);
        T record2 = getNextRandomRecordWith(firstName2, secondName);

        addRecord(record1);
        addRecord(record2);

        api.delete(record2);

        for (char literal: record1.literals()) {
            softAssert.assertThat(api.get(literal).contains(record1)).as("not removed value").isTrue();
        }
        for (char literal: record2.literals()) {
            softAssert.assertThat(api.get(literal).contains(record2)).as("removed value").isFalse();
        }

        softAssert.assertAll();
    }

    // Checks '@' character in name.
    @Test(expected = NameAndSurnameCannotContainAtException.class)
    public void nameContainsAtCharacter() {
        T record = getNextRandomRecordWith("@Name", "Surname");
        addRecord(record);
    }
}