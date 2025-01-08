package com.xyzcorp;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyProducer {
    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws InterruptedException {
        final Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all"); //0 maybe once, 1 exactly once, "all" every message
        properties.put(ProducerConfig.RETRIES_CONFIG, 100);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 250);
        //properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 2000); // a message better make it there in 2s. if it doesnt get there, we will call it off and Exception

        //1. de-deplicate any duplicates
        //2. guarentee order delivery of message to kafka
        //3. there is no performance penalty
        //in the meta data, it contrains the producer id (pid), and the sequence id, 
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        //properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1000); //how big is your batch in bytes
        //properties.put(ProducerConfig.LINGER_MS_CONFIG, 500); // how long do want the batch to wait on the producer set.
        //properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); //mulitple messages in one batch and compress them. 

        //custom partitioner
        //properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.xyzcorp.MyPartitioner");

        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(properties)) {
            String stateString =
                "AK,AL,AZ,AR,CA,CO,CT,DE,FL,GA," +
                    "HI,ID,IL,IN,IA,KS,KY,LA,ME,MD," +
                    "MA,MI,MN,MS,MO,MT,NE,NV,NH,NJ," +
                    "NM,NY,NC,ND,OH,OK,OR,PA,RI,SC," +
                    "SD,TN,TX,UT,VT,VA,WA,WV,WI,WY";

            AtomicBoolean done = new AtomicBoolean(false);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                done.set(true);
            }));

            Random random = new Random();

            while (!done.get()) {
                String[] states = stateString.split(",");
                String state = states[random.nextInt(states.length)];
                int amount = random.nextInt(100000 - 50 + 1) + 50;

                ProducerRecord<String, Integer> producerRecord =
                    new ProducerRecord<>("my_orders", state, amount);

                //Asynchronous
                producer.send(producerRecord, (metadata, e) -> {
                    if (metadata != null) {
                        System.out.println(producerRecord.key());
                        System.out.println(producerRecord.value());

                        if (metadata.hasOffset()) {
                            System.out.format("offset: %d\n",
                                metadata.offset());
                        }
                        System.out.format("partition: %d\n",
                            metadata.partition());
                        System.out.format("timestamp: %d\n",
                            metadata.timestamp());
                        System.out.format("topic: %s\n", metadata.topic());
                        System.out.format("toString: %s\n",
                            metadata.toString());
                    } else {
                        System.out.println("ERROR! ");
                        String firstException =
                            Arrays.stream(e.getStackTrace())
                                .findFirst()
                                .map(StackTraceElement::toString)
                                .orElse("Undefined Exception");
                        System.out.println(firstException);
                    }
                });

                Thread.sleep(random.nextInt(3000 - 100 + 1) + 1000);
            }
        } 
    }
}