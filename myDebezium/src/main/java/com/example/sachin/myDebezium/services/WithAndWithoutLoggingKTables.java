package com.example.sachin.myDebezium.services;

import com.example.sachin.myDebezium.util.StreamUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Service;

@Service
public class WithAndWithoutLoggingKTables extends AbstractDemoService {

    static enum LOGGING {
        ENABLED,
        DISABLED,
        DEFAULT
    }

    private final LOGGING LOGGING_VAL = LOGGING.DEFAULT;


    @Override
    protected void doExecute(final StreamsBuilder streamsBuilder) {
        switch (LOGGING_VAL) {
            case DEFAULT: this.loggingDefault(streamsBuilder);
                break;
            case ENABLED: this.loggingEnabled(streamsBuilder);
                break;
            case DISABLED: this.loggingDisabled(streamsBuilder);
                break;
            default: break;
        }
    }

    private void loggingEnabled(final StreamsBuilder streamsBuilder) {
        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> mat =
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("logging_enabled_user_sachin");
        final KTable<String, String> userStreamWithLogging = streamsBuilder.table(USER_TOPIC, Consumed.with(Serdes.String(), Serdes.String()), mat);
        StreamUtil.printStream("user-stream-logging-enabled", userStreamWithLogging.toStream());
    }

    private void loggingDisabled(final StreamsBuilder streamsBuilder) {
        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> mat =
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("this_is_sachin_disabled");
        mat.withLoggingDisabled();
        final KTable<String, String> userStreamWithoutLogging = streamsBuilder.table(USER_TOPIC, Consumed.with(Serdes.String(), Serdes.String()), mat);
        StreamUtil.printStream("something_log_disabled", userStreamWithoutLogging.toStream());
    }

    private void loggingDefault(final StreamsBuilder streamsBuilder) {
        final KTable<String, String> userStreamWithoutMatConfig = streamsBuilder.table(USER_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        StreamUtil.printStream("user-stream-without-mat-config", userStreamWithoutMatConfig.toStream());
    }
}
