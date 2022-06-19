package com.sachin.consumer.entity;


import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "user_address_tbl")
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
@ToString(exclude = "user")
public class Address implements Serializable {

    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    @Column
    private long id;

    @Column
    private String address;

    @Column
    private String city;

    @Column
    private int zip;



    @Transient
    private long userId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id")
    private User user;


}
