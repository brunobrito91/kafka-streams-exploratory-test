package com.vinsguru.kafkastreams.config;

import com.vinsguru.kafkastreams.processor.HalfNumberProcessor;
import com.vinsguru.kafkastreams.processor.OddNumberProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamConfig {

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.half-output}")
    private String halfTopic;

    @Value("${kafka.topic.even-output}")
    private String outputTopic;

    @Autowired
    private OddNumberProcessor oddNumberProcessor;

    private KafkaProperties kafkaProperties;

    @Autowired
    public KafkaStreamConfig(KafkaProperties kafkaProperties){
        this.kafkaProperties = kafkaProperties;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
    public StreamsBuilderFactoryBean myKStreamBuilder() {
        return new StreamsBuilderFactoryBean(kStreamsConfigs());
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> config = new HashMap<>();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaProperties.getClientId());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return new KafkaStreamsConfiguration(config);
    }

    @Bean
    public KStream<String, String> kStream(@Qualifier(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME) StreamsBuilder kStreamBuilder) throws Exception {
        KStream<String, String> stream = kStreamBuilder.stream(inputTopic);

        oddNumberProcessor.process(stream);
        stream
                .filter((k, v) -> new Long(v.replaceAll("\"", "")) % 2 == 0)
                .mapValues(v -> {
                    System.out.println("Squarting Even :: " + v);
                    return String.valueOf((new Long(v.replaceAll("\"", "")) * new Long(v.replaceAll("\"", ""))));
                })
                .to(outputTopic);

        stream.
                process(() -> new HalfNumberProcessor());

        kStreamBuilder.globalTable(
                outputTopic,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(
                        halfTopic /* table/store name */)
                        .withKeySerde(Serdes.String()) /* key serde */
                        .withValueSerde(Serdes.String()) /* value serde */
        );

        return stream;
    }
}