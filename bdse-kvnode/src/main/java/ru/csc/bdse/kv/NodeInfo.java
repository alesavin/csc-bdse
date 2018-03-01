package ru.csc.bdse.kv;

import ru.csc.bdse.util.Require;

import java.util.Objects;

/**
 * Represents node information
 *
 * @author alesavin
 */
public class NodeInfo {

    private final String name;
    private final NodeStatus status;

    NodeInfo(String name, NodeStatus status) {
        Require.nonNull(name, "null name");
        Require.nonNull(status, "null status");

        this.name = name;
        this.status = status;
    }

    public String getName() {
        return name;
    }

    public NodeStatus getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeInfo nodeInfo = (NodeInfo) o;
        return Objects.equals(name, nodeInfo.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "NodeInfo{" +
                "name='" + name + '\'' +
                ", status=" + status +
                '}';
    }
}
