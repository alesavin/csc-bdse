package ru.csc.bdse.app.v10.controller;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.csc.bdse.app.PhoneBookApi;
import ru.csc.bdse.app.Record;
import ru.csc.bdse.app.util.NameAndSurnameCannotContainAtException;
import ru.csc.bdse.app.v10.model.BookRecordV10;

import java.util.Optional;
import java.util.Set;

@RestController
@RequestMapping("/v1.0/person")
public class BookControllerV10 {
    private final PhoneBookApi phoneBookApi;

    public BookControllerV10(@Qualifier("PhoneBookServiceV10") final PhoneBookApi phoneBookApi) {
        this.phoneBookApi = phoneBookApi;
    }

    @RequestMapping(method = RequestMethod.POST)
    public String post(@RequestBody BookRecordV10 record) {
        phoneBookApi.put(record);
        return "OK";
    }

    @RequestMapping(method = RequestMethod.DELETE)
    public String deleteV1(@RequestParam("firstName") String firstName,
                           @RequestParam("secondName") String secondName) {
        Record record = new BookRecordV10(firstName, secondName);
        phoneBookApi.delete(record);
        return "OK";
    }

    @RequestMapping(method = RequestMethod.GET, value = "/{literal}")
    public Set<BookRecordV10> get(@PathVariable("literal") char literal) {
        return phoneBookApi.get(literal);
    }

    @ExceptionHandler(NameAndSurnameCannotContainAtException.class)
    @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
    public String handle(NameAndSurnameCannotContainAtException e) {
        return Optional.ofNullable(e.getMessage()).orElse("Name and surname cannot contain '@' character.");
    }
}
