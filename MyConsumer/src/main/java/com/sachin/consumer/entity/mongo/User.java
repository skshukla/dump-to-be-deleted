package com.sachin.consumer.entity.mongo;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Document("users")
@Data
@Slf4j
@Builder
@ToString
public class User {


    private String id;

    @Id
    private long userId;

    private String firstName;
    private String lastName;
    private Instant updatedDate;
    private Set<Address> addresses = new HashSet<>();

    public User addAddress(final Address address) {
        this.addresses = Objects.isNull(this.addresses) ? new HashSet<>() : this.addresses;
        if (this.addresses.contains(address)) {
            this.addresses.remove(address);
        }
        this.addresses.add(address);
        return this;
    }

    public static User getFromEntity(final com.sachin.consumer.entity.User user) {
        return User.builder().firstName(user.getFirstName()).lastName(user.getLastName()).userId(user.getId()).updatedDate(Instant.now()).build();
    }
}
