package com.sinosoft.earlywarn.sendflink.entity;

import java.util.Date;

public class User {

    private long id;
    private String username;
    private long timestamp;
    private Double money;

    public User(){}

    public User(long id, long timestamp, Double money) {
        this.id = id;
        this.timestamp = timestamp;
        this.money = money;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", username='" + username + '\'' +
                ", timestamp=" + timestamp +
                ", money=" + money +
                '}';
    }
}
