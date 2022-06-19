package com.example.sachin.myDebezium.services.demos;

import com.example.sachin.myDebezium.services.db.UserService;
import com.example.sachin.myDebezium.util.StreamUtil;
import com.example.sachin.myDebezium.vo.Address;
import com.example.sachin.myDebezium.vo.User;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Objects;

@Service
@Slf4j
public class WindowedStreamSummarized2 {

    @Value("${topics.user}")
    protected String USER_TOPIC;

    @Value("${topics.user-address}")
    protected String USER_ADDRESS_TOPIC;

    @Value("${topics.summarized.user-address-join}")
    private String TOPIC_SUMMARIZED_USER_ADDRESS_JOIN;

    protected Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    @Autowired
    private UserService userService;

    public void getUserAddressJoinStreamDirect(final StreamsBuilder streamsBuilder) {
//        log.info("user2222: " + this.userService.findById(2));
        final KStream<String, User> ksUser = streamsBuilder.stream(USER_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .filter( (k, v) -> Objects.nonNull(v))
                .map( (k, v) -> {
                    return KeyValue.pair(k,  GSON.fromJson(new JSONObject(v).toString(), User.class));
                })
                .peek( (k, v) -> {
                    log.info("[User] : [{}]", v);
                });

        final KStream<String, Address> ksAddress = streamsBuilder.stream(USER_ADDRESS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .filter( (k, v) -> Objects.nonNull(v))
                .map( (k, v) -> {
                    final Address address = GSON.fromJson(new JSONObject(v).toString(), Address.class);
                    return KeyValue.pair(String.valueOf(address.getUserId()),  address);
                })
                .peek( (k, v) -> {
                    log.info("[Address] : [{}]", v);
                });

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

        kFinalResult.foreach( (k, v) -> {
            log.info("Inside for each stream key  {{}}, value {{}}", k, v);
//            log.info("user3333: " + this.userService.findById(2));
//            log.info("user resource {{}}", this.userService.findById(Long.valueOf(k)));
        });

        kFinalResult.to(TOPIC_SUMMARIZED_USER_ADDRESS_JOIN, Produced.with(Serdes.String(), Serdes.String()));
        StreamUtil.printStream(TOPIC_SUMMARIZED_USER_ADDRESS_JOIN, kFinalResult);
    }
}
