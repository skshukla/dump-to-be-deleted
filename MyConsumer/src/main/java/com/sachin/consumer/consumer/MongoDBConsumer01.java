package com.sachin.consumer.consumer;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sachin.consumer.entity.Address;
import com.sachin.consumer.entity.User;
import com.sachin.consumer.repository.mongo.MongoUserRepository;
import com.sachin.consumer.service.kafka.KafkaService;
import com.sachin.consumer.vo.UniqueUserAddressIds;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Component
@Slf4j
@Async
public class MongoDBConsumer01 implements AppConsumer {


    @Value("${topics.user_address_topics}")
    private List<String> TOPICS;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactory;

    @Autowired
    private KafkaService kafkaService;

    @Autowired
    private MongoUserRepository mongoUserRepository;

    @Autowired
    private MongoTemplate mongoTemplate;


    private Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    @Override
    public void consume() {
        log.info("MongoDBConsumer01#consume() method invoked, going to consume topics : {{}}", String. join(",", TOPICS ));
        final Consumer<?, ?> consumer = this.concurrentKafkaListenerContainerFactory.getConsumerFactory().createConsumer();
        consumer.subscribe(TOPICS);
        while (true) {
            final ConsumerRecords<?, ?> records = consumer.poll(Duration.ofSeconds(2));
            log.info("This run, got {{}} number of records", records.count());
            if (records.count() == 0) {
                continue;
            }

            for (ConsumerRecord<?, ?> record : records) {
                log.info("topic = {{}}, offset = {{}}, key = {{}}, value = {{}}", record.topic(), record.offset(), record.key(), record.value());
                this.handleKafkaMessageForMongo(record);
            }
        }
    }

    private void handleKafkaMessageForMongo(final ConsumerRecord<?, ?> record) {
        if (record.topic().endsWith("user_tbl")) {
            final User user = GSON.fromJson(record.value().toString(), User.class);
            this.handleUser(user);
        } else if (record.topic().endsWith("user_address_tbl")) {
            final Address address = GSON.fromJson(record.value().toString(), Address.class);
            this.handleAddress(address);
        } else {
            log.error("Invalid for topic {{}}", record.topic());
        }
    }

    private void handleAddress(final Address address) {
        final com.sachin.consumer.entity.mongo.User user = this.mongoUserRepository.findById(address.getUserId()).get();
        log.info("User from DB is : {{}}", user);

        this.mongoTemplate.updateFirst(
                Query.query(Criteria.where("_id")
                        .is(address.getUserId())
                        .andOperator(Criteria.where("addresses")
                                .elemMatch(Criteria.where("_id").is(address.getId())))),
                new Update().set("addresses.$", com.sachin.consumer.entity.mongo.Address.getFromEntity(address)), "users");
    }

    private void handleUser(final User user) {
        final com.sachin.consumer.entity.mongo.User userFromEntity = com.sachin.consumer.entity.mongo.User.getFromEntity(user);

        final Optional<com.sachin.consumer.entity.mongo.User> userFromDBOptional = this.mongoUserRepository.findById(user.getId());
        if (userFromDBOptional.isEmpty()) {
            this.mongoUserRepository.save(userFromEntity);
        } else {
            userFromEntity.setAddresses(userFromDBOptional.get().getAddresses());
            this.mongoUserRepository.save(userFromEntity);
        }
    }
}
