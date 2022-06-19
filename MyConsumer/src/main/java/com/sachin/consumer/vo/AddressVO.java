package com.sachin.consumer.vo;


import com.sachin.consumer.entity.User;
import lombok.*;

import javax.persistence.*;
import java.io.Serializable;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class AddressVO implements Serializable {
    private String address;
    private String city;
    private int zip;
}
