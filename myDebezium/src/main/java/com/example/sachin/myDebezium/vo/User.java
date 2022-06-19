package com.example.sachin.myDebezium.vo;

import com.google.gson.Gson;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

@NoArgsConstructor
@AllArgsConstructor
@Slf4j
@Builder
@Data

public class User {
    private int id;

    private String firstName;

    private String lastName;

    private String email;

    @Override
    public int hashCode() {
        return  new Integer(id).hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return  this.id == ((User)other).getId();
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
