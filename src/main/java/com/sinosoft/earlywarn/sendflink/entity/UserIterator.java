package com.sinosoft.earlywarn.sendflink.entity;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;

public class UserIterator implements Iterator<User>,Serializable{

    private static final long serialVersionUID = 1L;

    private  Random randomId = new Random();
    private  Random randomMoney = new Random();

    @Override
    public boolean hasNext() {
       return true;
    }

    @Override
    public User next() {
        return getUser();
    }

    public User getUser() {
        int id = randomId.nextInt(10);
        int money = randomMoney.nextInt(1000);
        Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());
        return new User(id,timestamp.getTime(),new Double(money));
    }
}

