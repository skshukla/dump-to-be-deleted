package com.sachin.consumer;


import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sachin.consumer.consumer.AppConsumer;
import com.sachin.consumer.consumer.MongoDBConsumer01;
import com.sachin.consumer.entity.Address;
import com.sachin.consumer.entity.User;
import com.sachin.consumer.repository.AddressRepository;
import com.sachin.consumer.repository.UserRepository;
import com.sachin.consumer.service.UserService;
import com.sachin.consumer.vo.AddressVO;
import com.sachin.consumer.vo.UniqueUserAddressIds;
import com.sachin.consumer.vo.UserVO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;



// mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=8089"

@SpringBootApplication
@EnableKafka
@Slf4j
@EnableAsync
@EnableMongoRepositories
public class MyConsumerApp implements CommandLineRunner {

    @Autowired
    private UserService userService;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private AddressRepository addressRepository;

    @Autowired
    private KafkaTemplate<String, UserVO> kafkaTemplate;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactory;

    protected Gson GSON = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();


    @Autowired
    @Qualifier("userAddressForForeignKeyConsumer")
    private AppConsumer userAddressForForeignKeyConsumer;

    @Autowired
    @Qualifier("userAddressForUniqueIdsConsumer")
    private AppConsumer userAddressForUniqueIdsConsumer;


    @Autowired
    @Qualifier("mongoDBConsumer01")
    private AppConsumer mongoDBConsumer01;

    @Autowired
    private ApplicationContext applicationContext;

    public static void main(String[] args) {
        SpringApplication.run(MyConsumerApp.class, args);
    }


    @Override
    public void run(String... args) throws Exception {
        log.info("Going to print all beans");
        Arrays.stream(this.applicationContext.getBeanDefinitionNames()).forEach(e -> {
            log.info("Bean {{}}", e);
        });

         // Code to trigger the consumer for User Address for Foreign-keys.
//        this.userAddressForForeignKeyConsumer.consume();

        // Code to trigger the consumer for User Address for Unique Ids.
//        this.userAddressForUniqueIdsConsumer.consume();


        this.mongoDBConsumer01.consume();


        log.info("Going to wait for program to finish");
        Thread.currentThread().sleep(5 * 60 * 1000);
    }


    private void getSampleRecord() {
        final User user2 = this.userRepository.findById(2L).get();
        System.out.println(user2.getFirstName() + ", " + user2.getLastName() + ", " + user2.getId());
        System.out.println(user2.getAddressList());
    }

    private void sampleInsert() {
        final Address a1 = Address.builder().address("A").zip(2323).city("SG").build();
        final Address a2 = Address.builder().address("B").zip(2324).city("SG2").build();
        final User user = User.builder().email("a@aaaa.aom").firstName("aaaa").lastName("hhhh").addressList(Arrays.asList(a1, a2)).build();
        a1.setUser(user);
        a2.setUser(user);

        final User u = userRepository.save(user);
    }

    @KafkaListener(topics = "summarized-user-address-join-022")
    public void listen(final ConsumerRecord<?, ?> record) {
        final User user = this.userRepository.findById(Long.parseLong(record.value().toString())).get();
        log.info("User Record {{}}", user);

        final UserVO userVO = this.getUserVO(user);

        final ListenableFuture<SendResult<String, UserVO>> f = this.kafkaTemplate.send("t-final", String.valueOf(user.getId()), userVO);
        f.addCallback((r -> {
            log.info("Stored successfully to offset {{}} for user {{}}", r.getRecordMetadata().offset(), r.getProducerRecord().value().getId());
        }), (ex) -> {
            log.error("Exception {{}} for user {{}}", ex.getMessage(), user.getId());
        });
    }


    @KafkaListener(topics = "final-user-address-uniq-ids")
    public void listen3(final ConsumerRecord<String, ?> record) {
        final UniqueUserAddressIds uniqueUserAddressIds = GSON.fromJson(record.value().toString(), UniqueUserAddressIds.class);
        log.info("Got message {{}}", uniqueUserAddressIds);
        this.publishMessageToKafkaForUniqueUserAddressIds(uniqueUserAddressIds);
    }

    private void publishMessageToKafkaForUniqueUserAddressIds(UniqueUserAddressIds uniqueUserAddressIds) {
        final Iterable<Address> addressIterable = this.addressRepository.findAllById(uniqueUserAddressIds.getAddressIds());
        final Set<Long> userIds = new HashSet<>(StreamSupport.stream(addressIterable.spliterator(), false).map(e -> e.getUser().getId()).collect(Collectors.toList()));

        userIds.addAll(uniqueUserAddressIds.getUserIds());


        log.info("All User ids {{}}", userIds);

        StreamSupport.stream(this.userRepository.findAllById(userIds).spliterator(), false).map(e -> this.getUserVO(e)).forEach(e -> {
            final ListenableFuture<SendResult<String, UserVO>> f = this.kafkaTemplate.send("t-final-2", String.valueOf(e.getId()), e);
            f.addCallback((r -> {
                log.info("Stored successfully to offset {{}} for user {{}}", r.getRecordMetadata().offset(), r.getProducerRecord().value().getId());
            }), (ex) -> {
                log.error("Exception {{}} for user {{}}", ex.getMessage(), e.getId());
            });
        });
    }


    private UserVO getUserVO(final User user) {
        final List<AddressVO> addressVOList = user.getAddressList().stream().map(e -> AddressVO.builder().address(e.getAddress()).city(e.getCity()).zip(e.getZip()).build()).collect(Collectors.toCollection(ArrayList::new));
        final UserVO userVO = UserVO.builder().id(user.getId()).firstName(user.getFirstName()).lastName(user.getLastName()).addressList(addressVOList).build();
        return userVO;
    }
}
