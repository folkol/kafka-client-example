package com.folkol.lab.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "indexers");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(
                    Collections.singletonList("ze.changeList"),
                    new ConsumerRebalanceListener() {
                        @Override
                        public void onPartitionsRevoked(Collection<TopicPartition> collection) {

                        }

                        @Override
                        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                            /*
                             /usr/local/opt/kafka/bin/kafka-consumer-groups \
                                --bootstrap-server localhost:9092 \
                                --topic ze.changeList \
                                --group indexers \
                                --reset-offsets \
                                --to-offset 0 \
                                --execute
                             */
                            if (args.length > 0 && args[0].equals("reindex")) {
                                consumer.seekToBeginning(collection);
                            }
                        }
                    });

            while (true) {
                var records = consumer.poll(Duration.ofSeconds(10));
                if (records.isEmpty()) {
                    break;
                }
                records.forEach(record -> {
                    System.out.printf("%s (@%d): %s%n", record.key(), record.offset(), record.value());
                });
            }
        }
    }
}
