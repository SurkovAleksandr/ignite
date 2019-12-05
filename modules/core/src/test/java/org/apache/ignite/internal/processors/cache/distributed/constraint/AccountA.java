package org.apache.ignite.internal.processors.cache.distributed.constraint;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

public class AccountA extends Account {
    @AffinityKeyMapped
    private String affinityKey = AccountA.class.getName();

    public AccountA(long id, String name, long account) {
        super(id, name, account);
    }
}
