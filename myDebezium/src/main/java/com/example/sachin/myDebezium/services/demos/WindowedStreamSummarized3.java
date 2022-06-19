package com.example.sachin.myDebezium.services.demos;

import com.example.sachin.myDebezium.services.AbstractDemoService;
import com.example.sachin.myDebezium.services.db.UserService;
import com.example.sachin.myDebezium.util.StreamUtil;
import com.example.sachin.myDebezium.vo.Address;
import com.example.sachin.myDebezium.vo.UniqueUserAddressIds;
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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

@Service
@Slf4j
public class WindowedStreamSummarized3 extends AbstractDemoService {

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

    @Override
    protected void doExecute(StreamsBuilder streamsBuilder) {
        log.info("Coming summarized 3");
        this.getUserAddressUniqueIds(streamsBuilder);
    }


    public void getUserAddressUniqueIds(final StreamsBuilder streamsBuilder) {
        final KStream<String, User> ksUser = streamsBuilder.stream(USER_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((k, v) -> Objects.nonNull(v))
                .mapValues((k, v) -> GSON.fromJson(new JSONObject(v).toString(), User.class))
                .groupByKey(Grouped.<String, User>with(Serdes.String(), StreamUtil.getSerde(new User())))
                .reduce((k, v) -> v, Materialized.<String, User, KeyValueStore<Bytes, byte[]>>as("mat-kt-u0").withValueSerde(StreamUtil.getSerde(new User())).withLoggingDisabled())
                .toStream()
                .peek((k, v) -> {
                    log.info("[KS User] : [{}]", k);
                });


        final KStream<String, Address> ksAddress = streamsBuilder.stream(USER_ADDRESS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((k, v) -> Objects.nonNull(v))
                .mapValues((k, v) -> GSON.fromJson(new JSONObject(v).toString(), Address.class))
                .groupByKey(Grouped.<String, Address>with(Serdes.String(), StreamUtil.getSerde(new Address())))
                .reduce((k, v) -> v, Materialized.<String, Address, KeyValueStore<Bytes, byte[]>>as("mat-kt-u1").withValueSerde(StreamUtil.getSerde(new Address())).withLoggingDisabled())
                .toStream()
                .peek((k, v) -> {
                    log.info("[KS Address] : [{}]", k);
                });

        final String key = "key";

        final KStream<String, UniqueUserAddressIds> ksJoined = ksUser.outerJoin(ksAddress, new ValueJoiner<User, Address, UniqueUserAddressIds>() {

                            @Override
                            public UniqueUserAddressIds apply(final User user, final Address address) {
                                final Set<Long> userSet = new HashSet<>();
                                final Set<Long> addressSet = new HashSet<>();
                                if (Objects.nonNull(user) && Objects.nonNull(address)) {
                                    throw new RuntimeException("For this Demo, this case should not occur, if it occurs, explicitly throw exception");
                                }
                                if (Objects.nonNull(user)) {
                                    userSet.add(new Long(user.getId()));
                                    return UniqueUserAddressIds.builder().userIds(userSet).build();
                                }
                                else if (Objects.nonNull(address)) {
                                    addressSet.add(new Long(address.getId()));
                                    return UniqueUserAddressIds.builder().addressIds(addressSet).build();
                                }
                                throw new RuntimeException("For this Demo, this case also should not occur, if it occurs, explicitly throw exception");
                            }
                        }
                        , JoinWindows.of(Duration.ofSeconds(5))
                        , StreamJoined.<String, User, Address>with(Serdes.String(), StreamUtil.getSerde(new User()), StreamUtil.getSerde(new Address())))
                .selectKey( (k, v) -> key);

        StreamUtil.printStream("ksJoined", ksJoined);

                final TimeWindowedKStream<String, UniqueUserAddressIds> timedWindowedKStream = ksJoined
                        .groupByKey(Grouped.<String, UniqueUserAddressIds>with(Serdes.String(), StreamUtil.getSerde(new UniqueUserAddressIds())))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(0)));

        final KStream<String, UniqueUserAddressIds> kFinalResult = timedWindowedKStream
                .aggregate(() -> UniqueUserAddressIds.builder().build(), (k, v, a) -> {
                    final Set<Long> set = Objects.nonNull(a.getUserIds()) ? a.getUserIds() : new HashSet<>();
                    if (Objects.nonNull(v.getUserIds())) {
                        set.addAll(v.getUserIds());
                    }
                    a.setUserIds(set);

                    final Set<Long> set2 = Objects.nonNull(a.getAddressIds()) ? a.getAddressIds() : new HashSet<>();
                    if (Objects.nonNull(v.getAddressIds())) {
                        set2.addAll(v.getAddressIds());
                    }
                    a.setAddressIds(set2);

                    return a;
                }, Materialized.<String, UniqueUserAddressIds, WindowStore<Bytes, byte[]>>as("mat-final-ua").withValueSerde(StreamUtil.getSerde(new UniqueUserAddressIds())))
                .toStream().selectKey( (k, v) -> String.valueOf(k.key()));

        kFinalResult.to("final-user-address-uniq-ids", Produced.with(Serdes.String(), StreamUtil.getSerde(new UniqueUserAddressIds())));
        StreamUtil.printStream("final-user-address-uniq-ids", kFinalResult);


//
//
//        final KGroupedStream<String, String> kgsAddress = streamsBuilder.stream(USER_ADDRESS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
//                .filter((k, v) -> Objects.nonNull(v))
//                .mapValues( (k, v) -> "NA")
//                .groupByKey(Grouped.<String, String>with(Serdes.String(), Serdes.String()));
//
//
//
//
//        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> mat = Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("mat-kt-u1").withValueSerde(Serdes.String()).withLoggingDisabled();
//
//        final KTable<String, String> ktUser2 = ktUser
//                .groupBy( (k, v) -> KeyValue.pair(k, v))
//                .reduce( (k, v) -> v, (k, v) -> v, mat)
//        ;


//        KTable<Long, String> kt = ktUser.groupByKey(Grouped.<Long, String>with(Serdes.Long(), Serdes.String())).reduce(new Reducer<String>() {
//            @Override
//            public String apply(String s, String v1) {
//                return v1;
//            }
//        });

//        final KStream<Long, String> ksAddress = streamsBuilder.stream(USER_ADDRESS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
//                .filter( (k, v) -> Objects.nonNull(v))
//                .map( (k, v) -> {
//                    return KeyValue.pair(Long.parseLong(k), "NA");
//                })
//                .peek( (k, v) -> {
//                    log.info("[Address] : [{}]", k);
//                });


//        final KStream<String, String> ksUserAddressJoin = ksAddress.outerJoin(ksTable,  (a, u) -> Objects.nonNull(u) ? String.valueOf(u.getId()) : Objects.nonNull(a) ? String.valueOf(a.getUserId()) : "-1"
//                , JoinWindows.of(Duration.ofSeconds(5))
//                , StreamJoined.<String, Address, User>with(Serdes.String(), StreamUtil.getSerde(new Address()), StreamUtil.getSerde(new User())));
//
//        final TimeWindowedKStream<String, String> timedWindowedKStream = ksUserAddressJoin.groupByKey(Grouped.<String, String>with(Serdes.String(), Serdes.String()))
//                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(0)));
//
//        final KStream<String, String> kFinalResult = timedWindowedKStream
//                .aggregate(() -> "", (k, v, a) -> {
//                    return v;
//                }, Materialized.<String, String, WindowStore<Bytes, byte[]>>as("mat-final").withValueSerde(Serdes.String()))
//                .toStream().selectKey( (k, v) -> String.valueOf(k.key()));
//
//        kFinalResult.foreach( (k, v) -> {
//            log.info("Inside for each stream key  {{}}, value {{}}", k, v);
////            log.info("user3333: " + this.userService.findById(2));
////            log.info("user resource {{}}", this.userService.findById(Long.valueOf(k)));
//        });
//
//        kFinalResult.to(TOPIC_SUMMARIZED_USER_ADDRESS_JOIN, Produced.with(Serdes.String(), Serdes.String()));
//        StreamUtil.printStream(TOPIC_SUMMARIZED_USER_ADDRESS_JOIN, kFinalResult);
    }


}
