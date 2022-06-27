package com.example.rabbit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    @Autowired
    private Sender sender;

    @Autowired
    private StreamBridge streamBridge;

    @PostMapping("/test/{element}")
    public void test(Person person, @PathVariable String element) {
        streamBridge.send(StreamProcessor.TEST_JOB_OUTPUT, MessageBuilder.withPayload("Test Message")
                .setHeader("ROUTING-KEY", element).build());
    }

    @PostMapping("/test2/{element}")
    public void test2(Person person, @PathVariable String element) {
        streamBridge.send(StreamProcessor.TEST_JOB_OUTPUT_2, MessageBuilder.withPayload("Test2 Message")
                .setHeader("ROUTING-KEY", element).build());
    }
}
