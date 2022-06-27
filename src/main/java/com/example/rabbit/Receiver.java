package com.example.rabbit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class Receiver {
    Logger logger = LoggerFactory.getLogger(Receiver.class.getName());

    @Bean
    public Consumer<String> processDeviceEvent1() {
        return (value) -> {
            logger.info("1 - " + value);
        };
    }

    @Bean
    public Consumer<String> processDeviceEvent2() {
        return (value) -> {
            logger.info("2 - " + value);
        };
    }
}
