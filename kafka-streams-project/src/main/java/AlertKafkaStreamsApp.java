import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Materialized;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreBuilder;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Properties;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.utils.Bytes;

public class AlertKafkaStreamsApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("application.id", "alert-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("schema.registry.url", "http://localhost:8081");

        // Настройка Avro Serde
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "io.confluent.kafka.streams.serdes.avro.GenericAvroSerde");

        StreamsBuilder builder = new StreamsBuilder();

        // Поток покупок
        KStream<String, GenericRecord> purchaseStream = builder.stream("purchase-topic");

        // Поток продуктов
        KTable<String, GenericRecord> productTable = builder.table("product-topic");

        // Объединение потоков по productid
        KStream<String, GenericRecord> joinedStream = purchaseStream
            .leftJoin(productTable, (purchase, product) -> {
                if (product == null) return null;
                return purchase;
            }, Joined.with(Serdes.String(), new GenericAvroSerde(), new GenericAvroSerde()))
            .filter((key, value) -> value != null);

        // Обработка и агрегация
        joinedStream
            .mapValues(value -> {
                // Извлекаем price и quantity из Avro-объекта
                long quantity = (Long) value.get("quantity");
                double price = (Double) value.get("price");
                return quantity * price;
            })
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofSeconds(60)))
            .aggregate(
                () -> 0.0,
                (key, value, aggregate) -> aggregate + value,
                (key, value, aggregate) -> aggregate,
                Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("alert-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Double())
            )
            .filter((key, value) -> value > 3000.0)
            .toStream()
            .to("alert-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}