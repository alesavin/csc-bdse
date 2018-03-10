package ru.csc.bdse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import ru.csc.bdse.kv.BerkleyKeyValueApi;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.util.Env;

import java.util.UUID;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    private static String randomNodeName() {
        return "kvnode-" + UUID.randomUUID().toString().substring(4);
    }

    @Bean
    @Primary
    @Autowired
    KeyValueApi node(BerkleyKeyValueApi berkleyKeyValueApi) {
        String nodeName = Env.get(Env.KVNODE_NAME).orElseGet(Application::randomNodeName);
        berkleyKeyValueApi.setNodeName(nodeName);
        return berkleyKeyValueApi;
    }
}
