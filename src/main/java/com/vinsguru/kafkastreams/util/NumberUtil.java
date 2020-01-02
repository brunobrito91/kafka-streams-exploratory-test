package com.vinsguru.kafkastreams.util;

import com.vinsguru.kafkastreams.config.KafkaStreamConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class NumberUtil {

    @Autowired
    private KafkaStreamConfig kafkaStreamConfig;

    @Value("${kafka.topic.half-output}")
    private String halfTopic;

    public String findHalfNumberByNumber(String number) {
        ReadOnlyKeyValueStore<String, String> keyValueStore = kafkaStreamConfig.myKStreamBuilder().getKafkaStreams().store(halfTopic, QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, String> range = keyValueStore.all();
        while (range.hasNext()) {
            KeyValue<String, String> next = range.next();
            System.out.println("count for " + next.key + ": " + next.value);
        }
        range.close();

        return number;
    }
}
