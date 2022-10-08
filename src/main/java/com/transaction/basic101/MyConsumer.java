package com.transaction.basic101;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;


@Component
public class MyConsumer {

    private final ObjectMapper mapper;
    private final static Logger logger = LoggerFactory.getLogger(MyConsumer.class);
    private final KafkaTemplate<String, String> kafkaTemplate;

    public MyConsumer(ObjectMapper mapper, KafkaTemplate<String, String> kafkaTemplate) {
        this.mapper = mapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    // consume -> processing -> write (Exactly once processing)
    @KafkaListener(
            id = "my-transaction-consumer",
            topics = "transaction-topic",
            groupId = "transaction-group",
            concurrency = "1"
    )
    @Transactional("transactionManager") // <----- database transaction manager bean
    public void listen(
            @Payload String payload
    ) throws InterruptedException {
        logger.info("""
                =======================================================================
                payload: {}
                =======================================================================
                """, payload);

        // send data to other topic
        kafkaTemplate.send("log-topic", "Processed successfully message: " + payload);

        // save in db -> Inbox design pattern
        // table -> inbox (eventId)
        // check in db if this eventId is already exist, it means we are receiving same event again

        Thread.sleep(10000L);

//        throw new RuntimeException("some error occurred");
    }

    @KafkaListener(
            id = "my-transaction-log-consumer",
            topics = "log-topic",
            groupId = "transaction-log-group",
            concurrency = "1"
    )
    public void listenLog(
            @Payload String payload
    ) {
        logger.info("""
                =======================================================================
                log payload: {}
                =======================================================================
                """, payload);

    }
}
