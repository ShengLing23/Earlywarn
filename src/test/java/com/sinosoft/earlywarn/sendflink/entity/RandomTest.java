package com.sinosoft.earlywarn.sendflink.entity;

import org.junit.jupiter.api.Test;

import java.util.Random;

public class RandomTest {

    @Test
    public void randomTest(){
        Random random = new Random();
        while (true){
            double v = random.nextDouble();
            System.out.println(v);
        }

    }
}
