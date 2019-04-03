package com.vcredit.kafka.producer;


import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemoCallback {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop2:9092,hadoop3:9092");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 20);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("partitioner.class", HashPartitioner.class.getName());

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 20; i++) {
            ProducerRecord record = new ProducerRecord<String, String>
                    ("topic2", "key:"+Integer.toString(i), "value:"+Integer.toString(i));
			//producer.send(record);
			producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					System.out.printf("Send record partition:%d, offset:%d, keysize:%d,valuesize:%d ,%n",
							metadata.partition(), metadata.offset(), metadata.serializedKeySize(),
							metadata.serializedValueSize());
				}

			});
			//lambda expression
/*            producer.send(record, (metadata, exception) -> {
                if(metadata != null) {
                    System.out.printf("Send record partition:%d, offset:%d, keysize:%d, valuesize:%d %n",
                            metadata.partition(), metadata.offset(), metadata.serializedKeySize(),
                            metadata.serializedValueSize());
                }
                if(exception != null) {
                    exception.printStackTrace();
                }
            });*/
        }
        producer.close();
    }

}
