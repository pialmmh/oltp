package com.telcobright.rtc.photoncache;

import net.openhft.chronicle.wire.Marshallable;

public class Account implements Marshallable {
    private int id;
    private String name;
    private int balance;

    public Account() {}  // Required for Chronicle deserialization

    public Account(int id, String name, int balance) {
        this.id = id;
        this.name = name;
        this.balance = balance;
    }

    public void applyDelta(Account delta) {
        this.balance += delta.balance;
    }

    // Getters and setters
    public int getId() { return id; }
    public String getName() { return name; }
    public int getBalance() { return balance; }

    public void setId(int id) { this.id = id; }
    public void setName(String name) { this.name = name; }
    public void setBalance(int balance) { this.balance = balance; }

    @Override
    public String toString() {
        return "Account{id=" + id + ", name='" + name + "', balance=" + balance + "}";
    }
}
