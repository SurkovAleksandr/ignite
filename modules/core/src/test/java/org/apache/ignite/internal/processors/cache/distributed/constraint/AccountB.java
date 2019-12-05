package org.apache.ignite.internal.processors.cache.distributed.constraint;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

public class AccountB extends Account {
    @AffinityKeyMapped
    private String affinityKey = AccountB.class.getName() + "B";

    public AccountB(long id, String name, long account) {
        super(id, name, account);
    }
}
