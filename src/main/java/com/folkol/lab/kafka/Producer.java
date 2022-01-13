package com.folkol.lab.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        if (args.length != 1) {
            System.err.println("usage: java com.folkol.lab.kafka.Producer NUM_ITEMS");
            System.exit(1);
        }
        long numMessages = Long.parseLong(args[0]);

        var producer = new KafkaProducer<String, String>(props);
        for (var i = 0; i < numMessages; i++) {
            System.out.println(i);
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            "ze.changeList",
                            Long.toString(i),
                            "Hello, world!");
            producer.send(record);
        }

        producer.close();
    }
}
