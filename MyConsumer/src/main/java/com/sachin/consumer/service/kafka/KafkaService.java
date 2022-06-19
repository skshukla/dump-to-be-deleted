package com.sachin.consumer.service.kafka;

import com.sachin.consumer.entity.Address;
import com.sachin.consumer.entity.User;
import com.sachin.consumer.repository.AddressRepository;
import com.sachin.consumer.repository.UserRepository;
import com.sachin.consumer.vo.AddressVO;
import com.sachin.consumer.vo.UniqueUserAddressIds;
import com.sachin.consumer.vo.UserVO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Service
@Slf4j
public class KafkaService {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private AddressRepository addressRepository;

    @Autowired
    private KafkaTemplate<String, UserVO> kafkaTemplate;

    public void publishMessageToKafkaForUniqueUserAddressIds(final UniqueUserAddressIds uniqueUserAddressIds) {
        log.info("Got uniqueUserAddressIds as {{}}", uniqueUserAddressIds);
        final Iterable<Address> addressIterable = this.addressRepository.findAllById(uniqueUserAddressIds.getAddressIds());
        final Set<Long> userIds = new HashSet<>(StreamSupport.stream(addressIterable.spliterator(), false).map(e -> e.getUser().getId())
                .collect(Collectors.toList()));

        userIds.addAll(uniqueUserAddressIds.getUserIds());


        log.info("All User ids {{}}", userIds);

        StreamSupport.stream(this.userRepository.findAllById(userIds).spliterator(), false)
                .map(e -> this.getUserVO(e)).forEach(e -> {
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
