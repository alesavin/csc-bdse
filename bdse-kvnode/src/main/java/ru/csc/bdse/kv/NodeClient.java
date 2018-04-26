package ru.csc.bdse.kv;

import feign.Headers;
import feign.Param;
import feign.RequestLine;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import ru.csc.bdse.model.KeyValueRecord;

import java.util.Set;

public interface NodeClient {
    @RequestLine("PUT /key-value-inner/{key}")
    String putInner(@Param("key") final String key, final byte[] value);

    @RequestLine("GET /key-value-inner/{key}")
    @Headers("Content-Type: application/json")
    KeyValueRecord getInner(@Param("key") final String key);

    @RequestLine("GET /key-value-inner?prefix={prefix}")
    Set<String> getsInner(@Param("prefix") final String prefix);

    @RequestLine("DELETE /key-value-inner/{key}")
    void delete(@Param("key") final String key);
}