package com.transaction.basic101;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.stereotype.Component;

import javax.persistence.EntityManagerFactory;

@Component
public class AppConfig {

    @Bean
    public ObjectMapper mapper() {
        return new ObjectMapper();
    }

    @Bean
    public NewTopic transactionTopic() {
        return TopicBuilder.name("transaction-topic")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic logTopic() {
        return TopicBuilder.name("log-topic")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    @Primary
    public JpaTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}
