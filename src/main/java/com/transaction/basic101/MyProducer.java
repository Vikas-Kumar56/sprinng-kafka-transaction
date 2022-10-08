package com.transaction.basic101;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class MyProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper;

    public MyProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
    }

    @Transactional("kafkaTransactionManager")
    public void sendMessage(String topic, String message) throws InterruptedException {
        kafkaTemplate.send(
                topic,
                message
        );

        kafkaTemplate.send(
                "log-topic",
                "Recieved " + message + " from: " + topic
        );

        Thread.sleep(10000L);
    }
}
