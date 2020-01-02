package com.vinsguru.kafkastreams.rest;

import com.vinsguru.kafkastreams.producer.KafkaProducer;
import com.vinsguru.kafkastreams.util.NumberUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaRest {

    private final KafkaProducer kafkaProducer;
    private NumberUtil numberUtil;

    @Autowired
    KafkaRest(KafkaProducer kafkaProducer, NumberUtil numberUtil) {
        this.kafkaProducer = kafkaProducer;
        this.numberUtil = numberUtil;
    }

    @PostMapping(value = "/publish")
    public void sendMessageToKafkaTopic(@RequestParam("message") String message,
                                        @RequestParam("topic") String topic) {
        this.kafkaProducer.sendMessage(topic, message);
    }

    @GetMapping(path = "/{number}")
    public String getNumberFromKafkaTable(@PathVariable("number") String number) {
        return this.numberUtil.findHalfNumberByNumber(number);
    }
}