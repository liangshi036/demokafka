package com.vcredit.kafka.consumer;


import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class DemoConsumerManualCommit {

    public static void main(String[] args) throws Exception {
        args = new String[] {"hadoop2:9092,hadoop3:9092", "topic2", "group2", "consumer4" };
        if (args == null || args.length != 4) {
            System.err.println(
                    "Usage:\n\tjava -jar kafka_consumer.jar ${bootstrap_server} ${topic_name} ${group_name} ${client_id}");
            System.exit(1);
        }
        String bootstrap = args[0];
        String topic = args[1];
        String groupid = args[2];
        String clientid = args[3];

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", groupid);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", DoubleDeserializer.class.getName());
        props.put("max.poll.interval.ms", "300000");
        props.put("max.poll.records", "500");
        props.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
//		atomiclong 可以理解是加了synchronized的long。
        AtomicLong atomicLong = new AtomicLong();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record -> {
                System.out.printf("client : %s , topic: %s , partition: %d , offset = %d, key = %s, value = %s%n",
                        clientid, record.topic(), record.partition(), record.offset(), record.key(), record.value());
                if (atomicLong.get() % 10 == 0) {
                    //同步调用。也就是commit成功才会返回。否则一直block
//					consumer.commitSync();
                }
            });
        }
    }

}
