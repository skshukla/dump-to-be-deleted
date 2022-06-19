package com.sachin.consumer.entity.mongo;

import lombok.*;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import org.springframework.data.annotation.Id;
import java.time.Instant;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class Address {

    @Id
    private long id;

    private String address;

    private String city;

    private int zip;

    private long userId;

    @Override
    public int hashCode() {

        return Long.valueOf(this.getId()).hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        return this.id == ((Address)other).getId();
    }

    public static Address getFromEntity(final com.sachin.consumer.entity.Address address) {
        return Address.builder().id(address.getId()).city(address.getCity()).address(address.getAddress()).zip(address.getZip()).userId(address.getUserId())
                .build();
    }

}
