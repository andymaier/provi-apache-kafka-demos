package de.predic8.j_serialization;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import de.predic8.b_offset.OffsetBeginningRebalanceListener;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static java.time.Duration.ofSeconds;

public class ArtikelConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(GROUP_ID_CONFIG, "a");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(SESSION_TIMEOUT_MS_CONFIG, "30000");

        KafkaConsumer<Long, Artikel> consumer = new KafkaConsumer<Long, Artikel>(props, new LongDeserializer(), new ArtikelSerde());
        consumer.subscribe(Arrays.asList("artikel"));//, new OffsetBeginningRebalanceListener(consumer, "artikel"));

        while(true) {

            ConsumerRecords<Long, Artikel> records = consumer.poll(ofSeconds(1));
            if (records.count() == 0)
                continue;

            for (ConsumerRecord<Long, Artikel> record : records)
                System.out.printf("offset= %d, key= %s, value= %s\n", record.offset(), record.key(), record.value());

        }
    }
}
