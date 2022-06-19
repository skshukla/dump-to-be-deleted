package com.example.sachin.myDebezium.consumer;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class MyConsumer implements InitializingBean {


    @KafkaListener(topics = "summarized-user-address-join-022", groupId = "g2443")
//    @KafkaListener(topics = "#{'${topics.summarized.user-address-join}'.split(',')}", groupId = "g2443")
    public void listen(final ConsumerRecord<?, ?> record) {
        System.out.println("Received Record : " + record);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("111111111111");
    }
}
