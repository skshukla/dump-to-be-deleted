package com.example.sachin.myDebezium.other;

import com.example.sachin.myDebezium.util.StreamUtil;
import com.example.sachin.myDebezium.vo.User;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.json.JSONObject;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Service
@Slf4j
public class MyTestStreamingApp implements InitializingBean {

    @Value("${topics.user}")
    protected String USER_TOPIC;

    @Autowired
    @Qualifier("streamingProperties")
    protected Properties applicationProperties;

    @Value("#{new Boolean('${settings.enable.increment.app.id}')}")
    private Boolean enableIncrementAppId;

    private Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    public void run() {
        this.worldCountSample();
//        this.userSample();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.applicationProperties.put("application.id", this.enableIncrementAppId ? this.applicationProperties.get("application.id") + "-" + System.currentTimeMillis() : this.applicationProperties.get("application.id"));
        this.printProps();
    }

    private void printProps() {
        this.applicationProperties.entrySet().stream().forEach(e -> {
            log.info("Inside MyTestStreamingApp#printProps(), Streaming App Properties Key [{}], Value [{}]", e.getKey(), e.getValue());
        });
    }

    private void userSample() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<String, String> stream = streamsBuilder.stream(USER_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, User> userStream = stream
                .filter( (k, v) -> Objects.nonNull(v))
                .map( (k, v) -> {
                    return KeyValue.pair(k, GSON.fromJson((new JSONObject(v)).toString(), User.class));
                });

        final String label = "t-raw---->" + USER_TOPIC;
        userStream.print(Printed.<String, User>toSysOut().withLabel(label));


        userStream
                .groupBy( (k, v) -> v.getEmail())
                .count(Materialized.as("WordCount"))
                .toStream()
                .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));




        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), this.applicationProperties);
        streams.start();
        try {
            Thread.sleep(5 * 60 * 1000);
        } catch (final InterruptedException e) {
            log.error("Error while sleeping [{}]", e.getMessage());
        }
        log.info("Exiting method CountStreamExample#run()......");

        streams.close();
    }

    private void worldCountSample() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> textLines = builder
                .stream("streams-plaintext-input");

        final KTable<String, Long> kt = textLines
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, value) -> value)
                .count(Materialized.as("WordCount"));

        StreamUtil.printStream("lbl", kt.toStream());

        kt.toStream()
                .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();

//        Properties props = new Properties();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-word-count-004");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kube0:30092");
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 3000);

        System.out.println("----");
        System.out.println(Serdes.String().getClass());
        final KafkaStreams streams = new KafkaStreams(topology, this.applicationProperties);
//        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(
                new Thread("streams-shutdown-hook") {
                    @Override
                    public void run() {
                        streams.close();
                        latch.countDown();
                    }
                });
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
