package com.example.sachin.myDebezium.vo;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.io.Serializable;
import java.util.*;

@NoArgsConstructor
@AllArgsConstructor
@Slf4j
@Builder
@Data
public class UserAddressJoin implements Serializable {
    private int id;

    private String firstName;

    private String lastName;

    private String email;

    private List<Address> addresses = new ArrayList<>();

    public void addAddress(final Address addr) {
        if (Objects.isNull(addr)) {
            return;
        }
        this.addresses.removeIf( a -> a.getId() == addr.getId());
        this.addresses.add(addr);
    }

    public static UserAddressJoin getInstance(final User user, final Address address) {
        return UserAddressJoin.builder()
                .id(user.getId())
                .firstName(user.getFirstName())
                .lastName(user.getLastName())
                .email(user.getEmail())
                .addresses(Arrays.asList(address))
                .build();
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
