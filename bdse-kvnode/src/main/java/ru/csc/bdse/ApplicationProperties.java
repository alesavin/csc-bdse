package ru.csc.bdse;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;
import ru.csc.bdse.partitioning.Partitioner;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Service
@ConfigurationProperties("bdse")
public class ApplicationProperties {
    private String dbfile;
    private String nodes;
    private int nodeTimeout;
    private int nodeWCL;
    private int nodeRCL;
    private String partitions;
    private String partitioner;

    private Partitioner partitionerInstance = null;

    public String getDbfile() {
        return dbfile;
    }

    public void setDbfile(String dbfile) {
        this.dbfile = dbfile;
    }

    public String[] getNodes() {
        // Splits by whitespace and commas.
        // Example: "test, test2,test3" => { "test", "test2", "test3" }
        if (this.nodes == null) {
            return new String[]{"http://localhost:8001"};
        }
        return this.nodes.split("(\\s|,)+");
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public String[] getPartitions() {
        // Splits by whitespace and commas.
        // Example: "test, test2,test3" => { "test", "test2", "test3" }
        if (this.partitions == null) {
            return new String[]{"http://localhost:8001"};
        }
        return this.partitions.split("(\\s|,)+");
    }

    public void setPartitions(String partitions) {
        this.partitions = partitions;
    }

    public Partitioner getPartitioner() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        if (this.partitionerInstance != null) {
            return this.partitionerInstance;
        }

        Class<? extends Partitioner> partitionerClass = Class.forName(
                "ru.csc.bdse.partitioning." + this.partitioner
        ).asSubclass(Partitioner.class);

        Set<String> partitions = new HashSet<>();
        Collections.addAll(partitions, this.getPartitions());
        this.partitionerInstance = partitionerClass.getDeclaredConstructor(Set.class).newInstance(partitions);

        return this.partitionerInstance;
    }

    public void setPartitioner(String partitioneer) {
        this.partitioner = partitioneer;
    }

    public int getNodeTimeout() {
        return nodeTimeout;
    }

    public void setNodeTimeout(int nodeTimeout) {
        this.nodeTimeout = nodeTimeout;
    }

    public int getNodeWCL() {
        return nodeWCL;
    }

    public void setNodeWCL(int nodeWCL) {
        this.nodeWCL = nodeWCL;
    }

    public int getNodeRCL() {
        return nodeRCL;
    }

    public void setNodeRCL(int nodeRCL) {
        this.nodeRCL = nodeRCL;
    }
}
