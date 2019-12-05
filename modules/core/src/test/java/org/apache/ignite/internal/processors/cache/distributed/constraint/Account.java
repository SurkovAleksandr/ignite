package org.apache.ignite.internal.processors.cache.distributed.constraint;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

public abstract class Account {
    private long id;
    private String name;
    //public для удобства инкремента/декремента
    public long account;

    public Account(long id, String name, long account) {
        this.id = id;
        this.name = name;
        this.account = account;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override public String toString() {
        return "Account{" +
            "id=" + id +
            ", name='" + name + '\'' +
            ", account=" + account +
            '}';
    }
}
