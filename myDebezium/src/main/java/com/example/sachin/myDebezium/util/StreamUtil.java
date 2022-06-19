package com.example.sachin.myDebezium.util;

import com.example.sachin.myDebezium.serdes.JsonDeserializer;
import com.example.sachin.myDebezium.serdes.JsonSerializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Windowed;

@Slf4j
public class StreamUtil {

    private static final Gson GSON_TO_PRINT = new GsonBuilder()
            .setPrettyPrinting()
            .create();

    public static <T> Serde<T> getSerde(T t) {
        final JsonSerializer<T> serializer = new JsonSerializer<>();
        final JsonDeserializer<T> deserializer = new JsonDeserializer<T>(t.getClass());
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static <K, V> void printStream(final String label, final KStream<K, V> stream) {
        stream.print((Printed.<K, V>toSysOut().withLabel(label)));
    }

    public static <K, V> void printStreamWindowed(final String label, final KStream<Windowed<K>, V> stream) {
        stream.print((Printed.<Windowed<K>, V>toSysOut().withLabel(label)));
    }
}
