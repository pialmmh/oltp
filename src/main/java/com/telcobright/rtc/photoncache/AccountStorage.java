package com.telcobright.rtc.photoncache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class AccountStorage {
    private final Map<Integer, Account> accountMap = new ConcurrentHashMap<>();

    public void initialize(Map<Integer, Account> initialAccounts) {
        accountMap.clear();
        accountMap.putAll(initialAccounts);
    }

    public Account get(int id) {
        return accountMap.get(id);
    }

    public boolean contains(int id) {
        return accountMap.containsKey(id);
    }

    public void applyDelta(int id, Account delta) {
        Account acc = accountMap.get(id);
        if (acc != null) {
            acc.applyDelta(delta);
        }
    }

    public Map<Integer, Account> snapshot() {
        return Map.copyOf(accountMap);
    }
}