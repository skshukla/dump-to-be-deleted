package com.example.sachin.myDebezium.services.demos;

import com.example.sachin.myDebezium.services.AbstractDemoService;
import com.example.sachin.myDebezium.util.StreamUtil;
import com.example.sachin.myDebezium.vo.Address;
import com.example.sachin.myDebezium.vo.User;
import com.example.sachin.myDebezium.vo.UserAddressJoin;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Objects;

@Service
@Slf4j
public class WindowedStream extends AbstractDemoService {

    @Value("${topics.summarized.user}")
    private String TOPIC_SUMMARIZED_USER;

    @Value("${topics.summarized.address}")
    private String TOPIC_SUMMARIZED_ADDRESS;

    @Value("${topics.summarized.user-address-join}")
    private String TOPIC_SUMMARIZED_USER_ADDRESS_JOIN;

    @Autowired
    private WindowedStreamSummarized windowedStreamSummarized;

    @Autowired
    private WindowedStreamSummarized2 windowedStreamSummarized2;

    protected Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();


//    @Override
//    protected void doExecute(final StreamsBuilder streamsBuilder) {
//        this.userAggregator(streamsBuilder);
//        this.addressAggregator(streamsBuilder);
//        this.windowedStreamSummarized.getUserAddressJoinStream(streamsBuilder);
//    }

    @Override
    protected void doExecute(final StreamsBuilder streamsBuilder) {
        this.windowedStreamSummarized2.getUserAddressJoinStreamDirect(streamsBuilder);
    }



    private void userAggregator(StreamsBuilder streamsBuilder) {
        final KStream<Integer, User> ksUser = this.getUserAsStream(streamsBuilder);

        final TimeWindowedKStream<Integer, User> timedWindowedKStream = ksUser.groupByKey(Grouped.<Integer, User>with(Serdes.Integer(), StreamUtil.getSerde(new User())))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(0)));

        final KStream<String, User> ktSummarizedUser = timedWindowedKStream

                .aggregate(() -> new User(), (k, v, a) -> {
                    log.info("Got the aggregator as {{}}, user as {{}}", a, v);
                    return v;
                }, Materialized.<Integer, User, WindowStore<Bytes, byte[]>>as("mat-user").withValueSerde(StreamUtil.getSerde(new User())).withLoggingDisabled())
//                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()).withName("suppress-window"))
                .toStream().selectKey( (k, v) -> String.valueOf(k.key()));

        StreamUtil.printStream(TOPIC_SUMMARIZED_USER, ktSummarizedUser);
        ktSummarizedUser.to(TOPIC_SUMMARIZED_USER, Produced.with(Serdes.String(), StreamUtil.getSerde(new User())));
    }

    private void addressAggregator(StreamsBuilder streamsBuilder) {
        final KStream<Integer, Address> ksAddress = this.getAddressAsStream(streamsBuilder);

        final TimeWindowedKStream<Integer, Address> timedWindowedKStream = ksAddress.groupByKey(Grouped.<Integer, Address>with(Serdes.Integer(), StreamUtil.getSerde(new Address())))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(0)));

        final KStream<String, Address> ktSummarizedAddress = timedWindowedKStream

                .aggregate(() -> new Address(), (k, v, a) -> {
                    log.info("Got the aggregator as {{}}, new Address as {{}}", a, v);
                    return v;
                }, Materialized.<Integer, Address, WindowStore<Bytes, byte[]>>as("mat-address").withValueSerde(StreamUtil.getSerde(new Address())).withLoggingDisabled())
                .toStream().selectKey( (k, v) -> String.valueOf(v.getId()));

        StreamUtil.printStream(TOPIC_SUMMARIZED_ADDRESS, ktSummarizedAddress);
        ktSummarizedAddress.to(TOPIC_SUMMARIZED_ADDRESS, Produced.with(Serdes.String(), StreamUtil.getSerde(new Address())));
    }
}
