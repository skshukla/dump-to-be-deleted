package com.example.sachin.myDebezium.services.demos;

import com.example.sachin.myDebezium.util.StreamUtil;
import com.example.sachin.myDebezium.vo.Address;
import com.example.sachin.myDebezium.vo.User;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Objects;

@Service
@Slf4j
public class WindowedStreamSummarized {

    @Value("${topics.summarized.user}")
    private String TOPIC_SUMMARIZED_USER;

    @Value("${topics.summarized.address}")
    private String TOPIC_SUMMARIZED_ADDRESS;

    @Value("${topics.summarized.user-address-join}")
    private String TOPIC_SUMMARIZED_USER_ADDRESS_JOIN;

    protected Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    public void getUserAddressJoinStream(final StreamsBuilder streamsBuilder) {
        final KStream<String, User> ksUser = streamsBuilder.stream(TOPIC_SUMMARIZED_USER, Consumed.with(Serdes.String(), StreamUtil.getSerde(new User())));
        final KStream<String, Address> ksAddress = streamsBuilder.stream(TOPIC_SUMMARIZED_ADDRESS, Consumed.with(Serdes.String(), StreamUtil.getSerde(new Address())))
                .selectKey( (k, v) -> String.valueOf(v.getUserId()));

        final KStream<String, String> ksUserAddressJoin = ksAddress.outerJoin(ksUser,  (a, u) -> Objects.nonNull(u) ? String.valueOf(u.getId()) : Objects.nonNull(a) ? String.valueOf(a.getUserId()) : "-1"
                , JoinWindows.of(Duration.ofSeconds(5))
                , StreamJoined.<String, Address, User>with(Serdes.String(), StreamUtil.getSerde(new Address()), StreamUtil.getSerde(new User())));

        final TimeWindowedKStream<String, String> timedWindowedKStream = ksUserAddressJoin.groupByKey(Grouped.<String, String>with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(0)));

        final KStream<String, String> kFinalResult = timedWindowedKStream
                .aggregate(() -> "", (k, v, a) -> {
                    return v;
                }, Materialized.<String, String, WindowStore<Bytes, byte[]>>as("mat-final").withValueSerde(Serdes.String()))
                .toStream().selectKey( (k, v) -> String.valueOf(k.key()));


        kFinalResult.to(TOPIC_SUMMARIZED_USER_ADDRESS_JOIN, Produced.with(Serdes.String(), Serdes.String()));
        StreamUtil.printStream(TOPIC_SUMMARIZED_USER_ADDRESS_JOIN, kFinalResult);
    }
}
