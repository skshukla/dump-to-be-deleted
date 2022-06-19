package com.example.sachin.myDebezium.vo;

import lombok.*;

import java.util.HashSet;
import java.util.Set;


@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
@ToString
public class UniqueUserAddressIds {

    private Set<Long> userIds = new HashSet<>();

    private Set<Long> addressIds = new HashSet<>();

}
