package com.sinosoft.earlywarn.sendflink.entity;

import org.junit.jupiter.api.Test;

public class UserIteratorTest {

    @Test
    public void printTest(){
        UserIterator iterator = new UserIterator();
        while (iterator.hasNext()){
            System.out.println(iterator.next().toString());
        }
    }
}
