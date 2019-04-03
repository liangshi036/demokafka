package com.vcredit.kafka.consumer;


import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class DemoConsumerAssign {

    public static void main(String[] args) {
        args = new String[]{"hadoop2:9092,hadoop3:9092", "topic2", "group3", "consumer1"};
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
        props.put("client.id", clientid);
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> topicPartitions = Arrays.asList(new TopicPartition(topic, 0), new TopicPartition(topic, 2));

        //使用了subscribe，就不能使用assign。反之亦然。
        consumer.assign( topicPartitions);
//        consumer.seek(new TopicPartition(topic, 2), 20);
//        consumer.seek(new TopicPartition(topic, 0), 25);

        for (TopicPartition tpr : topicPartitions) {
            consumer.seek(tpr, 25);
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100); //不会一直阻塞，100毫秒后再次来轮询
            records.forEach(record -> {
                System.out.printf("client : %s , topic: %s , partition: %d , offset = %d, key = %s, value = %s%n", clientid, record.topic(),
                        record.partition(), record.offset(), record.key(), record.value());
            });
        }

//        consumer.subscribe(Arrays.asList("test2"));
    }
}

