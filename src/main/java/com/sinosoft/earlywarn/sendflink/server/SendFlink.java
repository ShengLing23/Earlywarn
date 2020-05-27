package com.sinosoft.earlywarn.sendflink.server;

import com.sinosoft.earlywarn.sendflink.entity.User;
import com.sinosoft.earlywarn.sendflink.entity.UserIterator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.alibaba.fastjson.JSON;

import java.util.Properties;

public class SendFlink {

    private Properties kafkaProp = new Properties();
    private KafkaProducer<String,String> producer;

    public void init(){
        kafkaProp.put("bootstrap.servers","localhost:9092");
        kafkaProp.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProp.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer(kafkaProp);
    }
    
    private void send() throws InterruptedException {
        UserIterator iterator = new UserIterator();
        while (iterator.hasNext()){
            User next = iterator.next();
            ProducerRecord<String,String> record =
                    new ProducerRecord<>("flinkTest",null,null, JSON.toJSONString(next));
            producer.send(record);
            producer.flush();
            Thread.sleep(500);
        }
        
    }

    public static void main(String[] args) throws InterruptedException {
        SendFlink sendFlink = new SendFlink();
        sendFlink.init();
        sendFlink.send();
    }


}
