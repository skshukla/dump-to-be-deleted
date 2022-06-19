package com.example.sachin.myDebezium.services;

import ch.qos.logback.core.util.TimeUtil;
import com.example.sachin.myDebezium.util.StreamUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.*;
import org.springframework.stereotype.Service;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class WithAndWithoutLoggingKStreams extends AbstractDemoService {

    private static final String STORE_LOGGING_ENABLED = "logging_enabled_store";
    private static final String STORE_LOGGING_DISABLED = "logging_disabled_store";

    static enum LOGGING {
        ENABLED,
        DISABLED
    }

    private final LOGGING LOGGING_VAL = LOGGING.ENABLED;


    @Override
    protected void doExecute(final StreamsBuilder streamsBuilder) {
        switch (LOGGING_VAL) {
            case ENABLED: this.loggingEnabled(streamsBuilder);
                break;
            case DISABLED: this.loggingDisabled(streamsBuilder);
                break;
            default: break;
        }
    }

    private void loggingEnabled(final StreamsBuilder streamsBuilder) {
        final KStream<String, String> stream = streamsBuilder.stream(USER_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        final Materialized<String, Long, KeyValueStore<Bytes, byte[]>> mat = Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STORE_LOGGING_ENABLED);
        final KTable<String, Long> aggregatedCountKT = stream.groupByKey().count(mat);
        StreamUtil.printStream("user-updates-aggregated-count-logging-enabled", aggregatedCountKT.toStream());
    }

    private void loggingDisabled(final StreamsBuilder streamsBuilder) {
        final KStream<String, String> stream = streamsBuilder.stream(USER_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        final Materialized<String, Long, KeyValueStore<Bytes, byte[]>> mat = Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STORE_LOGGING_DISABLED)
                .withLoggingDisabled();
        final KTable<String, Long> aggregatedCountKT = stream.groupByKey().count(mat);
        StreamUtil.printStream("user-updates-aggregated-count-logging-disabled", aggregatedCountKT.toStream());
    }

    @Override
    protected void postStreamCreation(final KafkaStreams streams) {
        switch (LOGGING_VAL) {
            case ENABLED: this.dumpStoreInformation(streams, STORE_LOGGING_ENABLED);
                break;
            case DISABLED: this.dumpStoreInformation(streams, STORE_LOGGING_DISABLED);
                break;
            default: break;
        }
    }

    private void dumpStoreInformation(final KafkaStreams streams, final String storeName) {
        final ReadOnlyKeyValueStore<String, String> loggingEnabledStore = streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
        super.logKafkaStoreDataPeriodically(streams, storeName, loggingEnabledStore, 10, TimeUnit.SECONDS);
    }
}
