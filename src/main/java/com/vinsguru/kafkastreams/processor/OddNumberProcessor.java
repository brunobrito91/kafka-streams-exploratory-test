package com.vinsguru.kafkastreams.processor;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class OddNumberProcessor {

    @Value("${kafka.topic.odd-output}")
    private String oddOutput;

    public void process(KStream<String, String> stream){
        stream
                .filter((k, v) -> new Long(v.replaceAll("\"","")) % 2 == 0)
                .mapValues(v -> {
                    System.out.println("Doubling Odd :: " + v);
                    return String.valueOf((new Long(v.replaceAll("\"","")) * 2));
                })
                .to(oddOutput);
    }

}