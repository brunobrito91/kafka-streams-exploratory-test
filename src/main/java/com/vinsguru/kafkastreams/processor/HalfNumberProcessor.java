package com.vinsguru.kafkastreams.processor;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.stereotype.Service;

@Service
public class HalfNumberProcessor extends AbstractProcessor<String, String> {

    @Override
    public void process(String key, String value) {
        System.out.println("key= " + key + " value= " + value);
        context().forward(key, value);
    }

}
