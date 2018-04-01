package ru.csc.bdse.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import ru.csc.bdse.kv.ReplicatedKeyValueApi;
import ru.csc.bdse.model.KeyValueRecord;

import java.util.*;

@RestController
@RequestMapping("/key-value")
public class PublicKeyValueApiController {

    private ReplicatedKeyValueApi replicatedKeyValueApi;

    @Autowired
    public PublicKeyValueApiController(ReplicatedKeyValueApi replicatedKeyValueApi) {
        this.replicatedKeyValueApi = replicatedKeyValueApi;
    }


    @RequestMapping(method = RequestMethod.PUT, value = "/{key}")
    public String putOuter(@PathVariable final String key,
                           @RequestBody final byte[] value) {
        return this.replicatedKeyValueApi.put(key, value);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/{key}")
    public byte[] getOuter(@PathVariable final String key) {
        Optional<KeyValueRecord> record = this.replicatedKeyValueApi.get(key);
        if (record.isPresent()) {
            return record.get().getData();
        }

        throw new IllegalStateException();
    }

}
