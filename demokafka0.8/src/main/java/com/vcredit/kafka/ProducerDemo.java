package com.vcredit.kafka;


import java.util.*;

import kafka.serializer.StringEncoder;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class ProducerDemo {

    static private final String TOPIC = "topic1";

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.11:9092,192.168.0.12:9092");
        props.put("serializer.class", StringEncoder.class.getName());
        props.put("partitioner.class", RoundRobinPartitioner.class.getName());
        props.put("producer.type", "sync");
        props.put("batch.num.messages", "1");
        props.put("queue.buffering.max.messages", "1000000");
        props.put("queue.enqueue.timeout.ms", "20000000");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);;


        long messageNo = 10;
        Random rnd = new Random();
        long startTime = System.currentTimeMillis();

        for (long nEvents = 0; nEvents < messageNo; nEvents++) {
            // Now build your message
            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = "{runtime:"+runtime + ",value:{"+ip+"}";

            // Finally write the message to the Broker
            // passing the IP as the partition key
            ProducerRecord<String, String> data = new ProducerRecord<>(TOPIC, ip, msg);
            producer.send(data ,new DemoCallBack(startTime, ip, msg));
            Thread.sleep(500);
        }
        producer.close();
    }

}

//通过发送回调可以知道有没有发送成功。

class DemoCallBack implements Callback {

    private final long startTime;
    private final String key;
    private final String message;
    private Map<String, Integer> keyPartitons = new HashMap<>();

    public DemoCallBack(long startTime, String key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            int partition = metadata.partition();
            if(keyPartitons.get(key) == null) {
                keyPartitons.put(key, partition);
            } else {
                if(keyPartitons.get(key) != partition) {
                    System.out.println("key " + key + ", before:" + keyPartitons.get(key) + ", after: " + partition);
                }
            }
            System.out.println("消息(" + key + ", " + message + ") " +
                    "发送到分区(" + metadata.partition() + "), " +
                    "偏移量(" + metadata.offset() + ") " +
                    "耗时 " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}