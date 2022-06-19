package com.sachin.consumer.vo;

import com.sachin.consumer.entity.Address;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class UserVO {

    private long id;
    private String firstName;
    private String lastName;
    private List<AddressVO> addressList = new ArrayList<>();
}
