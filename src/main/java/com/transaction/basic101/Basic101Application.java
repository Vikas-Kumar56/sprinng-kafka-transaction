package com.transaction.basic101;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.UUID;

@SpringBootApplication
public class Basic101Application {

	public static void main(String[] args) {
		SpringApplication.run(Basic101Application.class, args);
	}

	@Bean
	public ApplicationRunner runner(MyProducer producer) {
		return args -> {
			try {
				producer.sendMessage("transaction-topic", "Message " + UUID.randomUUID());
			} catch(Exception exception) {

			}
		};
	}
}
