package com.example.sachin.myDebezium.services;

import com.example.sachin.myDebezium.serdes.JsonDeserializer;
import com.example.sachin.myDebezium.serdes.JsonSerializer;
import com.example.sachin.myDebezium.util.Constants;
import com.example.sachin.myDebezium.util.StreamUtil;
import com.example.sachin.myDebezium.vo.Address;
import com.example.sachin.myDebezium.vo.User;
import com.example.sachin.myDebezium.vo.UserAddressJoin;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.*;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public abstract class AbstractDemoService {

    @Value("${topics.user}")
    protected String USER_TOPIC;

    @Value("${topics.user-address}")
    protected String USER_ADDRESS_TOPIC;

    @Autowired
    @Qualifier("streamingProperties")
    protected Properties applicationProperties;

    @Value("#{new Boolean('${settings.enable.increment.app.id}')}")
    private Boolean enableIncrementAppId;

    protected Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    protected abstract void doExecute(final StreamsBuilder streamsBuilder);

    protected void postStreamCreation(final KafkaStreams streams) {
        log.info("Do nothing, meant for child classes to do some activities once the streams are created");
    }

    public final void run() {
        log.info("Coming to run() method.");
        this.adjustAndPrintStreamingProperties();
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        this.addStores(streamsBuilder);
        this.doExecute(streamsBuilder);
        this.startStreamAndWaitBeforeClosing(streamsBuilder);
    }

    private void adjustAndPrintStreamingProperties() {
        this.applicationProperties.put("application.id", this.enableIncrementAppId ? this.applicationProperties.get("application.id") + "-" + System.currentTimeMillis() : this.applicationProperties.get("application.id"));
        this.applicationProperties.entrySet().stream().forEach(e -> {
            log.info("Streaming App Properties Key [{}], Value [{}]", e.getKey(), e.getValue());
        });
    }

    private void addStores(final StreamsBuilder streamsBuilder) {
        final StoreBuilder<KeyValueStore<Integer, Integer>> storeBuilder =
                Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(Constants.STORE_NAME_DEMO)
                        , Serdes.Integer(), Serdes.Integer());

        final Map<String, String> configMap = new HashMap<>();
        configMap.put("retention.ms", "1800000");
        configMap.put("delete.retention.ms", "100");
        configMap.put("segment.ms", "100");
        configMap.put("min.cleanable.dirty.ratio", "0.01");

        storeBuilder.withLoggingEnabled(configMap);
        streamsBuilder.addStateStore(storeBuilder);

        final StoreBuilder<KeyValueStore<String, String>> storeBuilder2 =
                Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(Constants.STORE_NAME_DEMO2)
                        , Serdes.String(), Serdes.String());
        streamsBuilder.addStateStore(storeBuilder2);

    }

    private void startStreamAndWaitBeforeClosing(final StreamsBuilder streamsBuilder) {
        final Topology topology = streamsBuilder.build();
        log.info("Describing topology!");
        log.info(topology.describe().toString());
        this.printAllStores(topology);
        final KafkaStreams streams = new KafkaStreams(topology, this.applicationProperties);
        this.setStateListener(streams);
        streams.start();
        this.printStreamingStatsPeriodically(streams, 5, TimeUnit.SECONDS);
        this.dumpKTableInformationPeriodically(streams);

        this.postStreamCreation(streams);
        log.info("Going to sleep!!!!");
        try {
            Thread.sleep(5 * 60 * 1000);
        } catch (final InterruptedException e) {
            log.error("Error while sleeping [{}]", e.getMessage());
        }
        log.info("Exiting method CountStreamExample#run()......");

        streams.close();
    }

    private void printAllStores(final Topology topology) {
        TopologyDescription tdesc = topology.describe();
        for (TopologyDescription.Subtopology sub : tdesc.subtopologies()) {
            for (TopologyDescription.Node node : sub.nodes()) {
                if (node instanceof org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.Processor) {
                    InternalTopologyBuilder.Processor proc = (InternalTopologyBuilder.Processor) node;
                    proc.stores().stream().forEach(e -> {
                        log.info("Store name [{}]", e);
                    });
                }
            }
        }
    }

    private void setStateListener (final KafkaStreams streams) {
        streams.setStateListener( (newState, oldState) -> {
            log.info("Re-balancing streams, going from Old State {{}} to new State {{}}", oldState, newState);
            if (newState == KafkaStreams.State.RUNNING && oldState ==
                    KafkaStreams.State.REBALANCING) {
                log.info("\n\n\n=======>Setting the query server to ready\n\n");
            } else if (newState != KafkaStreams.State.RUNNING) {
                log.error("\n\n\n=======>State [{}] not RUNNING, disabling the query server\n\n", newState);
            }
        });
    }

    private void dumpKTableInformationPeriodically(final KafkaStreams streams) {
        final ReadOnlyKeyValueStore<String, String> myStore = streams.store(StoreQueryParameters.fromNameAndType("test_topic_compacted", QueryableStoreTypes.keyValueStore()));
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate( () -> {

            log.info("\n-------------------> KTable dump [STARTS]\n");

            if (streams.state() == KafkaStreams.State.RUNNING) {
                log.info("Kafka streams is in RUNNING state");
                log.info("Store has approx [{}] number of entries", myStore.approximateNumEntries());
                myStore.all().forEachRemaining(e -> {
                    log.info("Store Entry [key: {}, val: {}]", e.key, e.value);
                });
            } else {
                log.info("Kafka streams is in [{}] state, skipping querying store", streams.state());
            }

            log.info("\n<->------------------ KTable dump [ENDS]\n");
        }, 0,10, TimeUnit.SECONDS);
    }

    private void printStreamingStatsPeriodically(final KafkaStreams streams, final int period, final TimeUnit timeUnit) {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate( () -> {
            log.info("Streaming application state is [{}]", streams.state());
        }, 0, period, timeUnit);
    }


    protected KStream<Integer, User> getUserAsStream(final StreamsBuilder streamsBuilder) {
        final KStream<Integer, JSONObject> stream = this.getRawStream(streamsBuilder, USER_TOPIC);

        return stream.mapValues( (v) -> {
            return GSON.fromJson(v.toString(), User.class);
        });
    }

    protected KStream<Integer, Address> getAddressAsStream(final StreamsBuilder streamsBuilder) {
        final KStream<Integer, JSONObject> stream = this.getRawStream(streamsBuilder, USER_ADDRESS_TOPIC);
        return stream.filter( (k, v) -> {
            return !String.valueOf(v).equals("null");
        }).mapValues( v -> GSON.fromJson(v.toString(), Address.class));
    }


    protected KTable<String, UserAddressJoin> getUserAddressJoinAsKTableWithUserAsID(final StreamsBuilder streamsBuilder) {
        final KStream<String, Address> ksAddress =
                this.getRawStream(streamsBuilder, USER_ADDRESS_TOPIC).map( (k, v) -> {
                    final Address address = GSON.fromJson(String.valueOf(v), Address.class);
                    return KeyValue.pair(String.valueOf(address.getUserId()), address);
                });

        StreamUtil.printStream("BB-address", ksAddress);


        ;

        final KGroupedStream<String, Address> ksAddressGrouped = ksAddress.groupByKey(Grouped.with(Serdes.String(), StreamUtil.getSerde(new Address())));



        final Materialized<String, UserAddressJoin, KeyValueStore<Bytes, byte[]>> mat =
                Materialized.<String, UserAddressJoin, KeyValueStore<Bytes, byte[]>>as("aaaa")
                        .withValueSerde(StreamUtil.getSerde(new UserAddressJoin()));

        final KTable<String, UserAddressJoin> kt = ksAddressGrouped.aggregate( () -> new UserAddressJoin(), (k, v, a) -> {
            a.setId(Integer.parseInt(k));
            a.addAddress(v);
            return a;
        }, mat);


        StreamUtil.printStream("BB-UAJ", kt.toStream());
        return kt;
    }


    private Serde<UserAddressJoin> getUserAddressJoinSerde() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(UserAddressJoin.class));
    }

    protected KTable<String, User> getUserAsKTable(final StreamsBuilder streamsBuilder) {

        final Map<String, String> configMap = new HashMap<>();
        configMap.put("retention.ms", "3600050");
        configMap.put("delete.retention.ms", "100");
        configMap.put("segment.ms", "100");
        configMap.put("min.cleanable.dirty.ratio", "0.01");

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> mat =
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("user_compacted");

        mat.withLoggingEnabled(configMap);

        final KTable<String, String> sUserOrig =
                streamsBuilder.table(USER_TOPIC, Consumed.with(Serdes.String(), Serdes.String()), mat);

        return sUserOrig.mapValues( (s) -> {
            return GSON.fromJson(s, User.class);
        });
    }



//    protected KStream<String, JSONObject> getPlainRawStream(final StreamsBuilder streamsBuilder, final String topic) {
//        final KStream<String, String> stream = streamsBuilder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()));
//        final KStream<Integer, JSONObject> returnedStream = stream
//                .filter( (k, v) -> Objects.nonNull(v))
//                .map( (k, v) -> {
//                    return KeyValue.pair(Integer.parseInt(k), new JSONObject(v));
//                });
//
//        final String label = "raw---->" + topic;
//        returnedStream.print(Printed.<Integer, JSONObject>toSysOut().withLabel(label));
//        return returnedStream;
//    }


      protected KStream<Integer, JSONObject> getRawStream(final StreamsBuilder streamsBuilder, final String topic) {
        final KStream<String, String> stream = streamsBuilder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<Integer, JSONObject> returnedStream = stream
                .filter( (k, v) -> Objects.nonNull(v))
                .map( (k, v) -> {
                    return KeyValue.pair(Integer.parseInt(k), new JSONObject(v));
                });

        final String label = "raw---->" + topic;
        returnedStream.print(Printed.<Integer, JSONObject>toSysOut().withLabel(label));
        return returnedStream;
    }

    void logKafkaStoreDataPeriodically(final KafkaStreams streams, final String storeName, final ReadOnlyKeyValueStore<?, ?> store, final int period, final TimeUnit timeUtil) {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate( () -> {

            if (streams.state() == KafkaStreams.State.RUNNING) {
                log.info("\n\n-------------------> KTable dump [STARTS]");
                log.info("Kafka streams is in RUNNING state");
                log.info("Store [{}] has approx [{}] number of entries", storeName, store.approximateNumEntries());
                store.all().forEachRemaining(e -> {
                    log.info("Store [{}] Entry [key: {}, val: {}]", storeName, e.key, e.value);
                });
                log.info("\n->------------------ KTable dump [ENDS]\n");
            } else {
                log.info("Kafka streams is in [{}] state, skipping querying store", streams.state());
            }


        }, 0, period, timeUtil);
    }
}