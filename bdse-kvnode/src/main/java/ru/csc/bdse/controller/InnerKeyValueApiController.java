package ru.csc.bdse.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import ru.csc.bdse.kv.BerkleyKeyValueApi;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;
import ru.csc.bdse.model.KeyValueRecord;
import ru.csc.bdse.util.IllegalNodeStateException;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

/**
 * Provides HTTP API for the storage unit
 *
 * @author semkagtn
 */
@RestController
@RequestMapping("/key-value-inner")
public class InnerKeyValueApiController {
    private final BerkleyKeyValueApi keyValueApi;

    @Autowired
    public InnerKeyValueApiController(final BerkleyKeyValueApi keyValueApi) {
        this.keyValueApi = keyValueApi;
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/{key}")
    public String putInner(@PathVariable final String key,
                           @RequestBody final byte[] value) {
        return keyValueApi.put(key, value);
    }


    @RequestMapping(method = RequestMethod.GET, value = "/{key}", produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody
    KeyValueRecord getInner(@PathVariable final String key) {
        KeyValueRecord keyValueRecord = keyValueApi.get(key)
                .orElseThrow(() -> new NoSuchElementException(key));
        return keyValueRecord;
    }

    @RequestMapping(method = RequestMethod.GET, value = "/info")
    public Set<NodeInfo> getInfo() {
        return keyValueApi.getInfo();
    }

    @RequestMapping(method = RequestMethod.POST, value = "/action/{node}/{action}")
    public void action(@PathVariable final String node,
                       @PathVariable final NodeAction action) {
        keyValueApi.action(node, action);
    }

    @RequestMapping(method = RequestMethod.GET)
    public Set<String> getKeys(@RequestParam("prefix") String prefix) {
        return keyValueApi.getKeys(prefix);
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "/{key}")
    public void delete(@PathVariable final String key) {
        keyValueApi.delete(key);
    }

    @ExceptionHandler(NoSuchElementException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public String handle(NoSuchElementException e) {
        return Optional.ofNullable(e.getMessage()).orElse("");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public String handle(IllegalArgumentException e) {
        return Optional.ofNullable(e.getMessage()).orElse("");
    }

    @ExceptionHandler(IllegalNodeStateException.class)
    @ResponseStatus(value = HttpStatus.SERVICE_UNAVAILABLE, reason = "IllegalNodeState")
    public String handle(IllegalNodeStateException e) {
        return Optional.ofNullable(e.getMessage()).orElse("IllegalNodeState");
    }
}
