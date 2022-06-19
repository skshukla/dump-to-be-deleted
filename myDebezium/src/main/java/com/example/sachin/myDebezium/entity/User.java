package com.example.sachin.myDebezium.entity;



import javax.persistence.*;

import lombok.*;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.springframework.cache.annotation.Cacheable;

@Entity
@Table(name = "user_tbl")
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
@ToString
public class User {
    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    @Column
    private long id;

    @Column(name = "first_name")
    private String firstName;


    @Column(name = "last_name")
    private String lastName;
}
