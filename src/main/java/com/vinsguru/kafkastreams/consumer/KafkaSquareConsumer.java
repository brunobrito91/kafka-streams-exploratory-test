package com.vinsguru.kafkastreams.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaSquareConsumer {

    @KafkaListener(topics = {"${kafka.topic.even-output}", "${kafka.topic.odd-output}"})
    public void consume(String number) {
        System.out.println(String.format("Consumed :: %s", number));
    }

}