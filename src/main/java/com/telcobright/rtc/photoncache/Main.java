package com.telcobright.rtc.photoncache;

import java.util.Map;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        AccountCache cache = new AccountCache();

        // ✅ Step 1: Initialize known accounts
        Map<Integer, Account> initialAccounts = Map.of(
                1, new Account(1, "Alice", 100),
                2, new Account(2, "Bob", 200)
        );
        cache.initializeAccounts(initialAccounts);

        // ✅ Step 2: Apply deltas
        cache.updateAccount(new Account(1, "Alice", 50));  // Alice +50
        cache.updateAccount(new Account(2, "Bob", -30));   // Bob -30
        cache.updateAccount(new Account(3, "Charlie", 300)); // Will be ignored and logged

        // ⏳ Give time for background thread to consume
        Thread.sleep(1000);

        // ✅ Step 3: Print final state
        System.out.println("Final Alice: " + cache.getAccount(1));
        System.out.println("Final Bob: " + cache.getAccount(2));

        // ✅ Step 4: Clean shutdown
        cache.shutdown();
        System.out.println("end");
    }
}
