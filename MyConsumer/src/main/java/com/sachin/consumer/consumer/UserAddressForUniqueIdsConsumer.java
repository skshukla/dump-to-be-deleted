package com.sachin.consumer.consumer;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sachin.consumer.entity.Address;
import com.sachin.consumer.entity.User;
import com.sachin.consumer.service.kafka.KafkaService;
import com.sachin.consumer.vo.UniqueUserAddressIds;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component
@Slf4j
@Async
public class UserAddressForUniqueIdsConsumer implements AppConsumer {

    private final List<String> topics = Arrays.asList("my-topics-06-partitioned.public.user_tbl", "my-topics-06-partitioned.public.user_address_tbl");

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactory;

    @Autowired
    private KafkaService kafkaService;

    private Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    @Override
    public void consume() {
        log.info("UserAddressForUniqueIdsConsumer#consume() method invoked.");
        final Consumer<?, ?> consumer = this.concurrentKafkaListenerContainerFactory.getConsumerFactory().createConsumer();
        consumer.subscribe(topics);
        while (true) {
            final ConsumerRecords<?, ?> records = consumer.poll(4000);
            log.info("This run, got {{}} number of records", records.count());
            if (records.count() == 0) {
                continue;
            }

            final Set<Long> userSet = new HashSet<>();
            final Set<Long> addressSet = new HashSet<>();
            for (ConsumerRecord<?, ?> record : records) {
                System.out.printf("topic = %s, offset = %d, key = %s, value = %s\n", record.topic(), record.offset(), record.key(), record.value());

                if ("my-topics-06-partitioned.public.user_tbl".equalsIgnoreCase(record.topic())) {
                    final User user = GSON.fromJson(record.value().toString(), User.class);
                    userSet.add(user.getId());
                } else if ("my-topics-06-partitioned.public.user_address_tbl".equalsIgnoreCase(record.topic())) {
                    final Address address = GSON.fromJson(record.value().toString(), Address.class);
                    addressSet.add(address.getId());
                } else {
                    log.error("Invalid for topic {{}}", record.topic());
                }
            }

            final UniqueUserAddressIds uniqueUserAddressIds = UniqueUserAddressIds.builder().userIds(userSet).addressIds(addressSet).build();

            this.kafkaService.publishMessageToKafkaForUniqueUserAddressIds(uniqueUserAddressIds);
        }
    }
}
