package com.sachin.consumer.repository.mongo;


import com.sachin.consumer.entity.mongo.User;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface MongoUserRepository extends MongoRepository<User, Long> {

//    @Query("{name:'?0'}")
//    User findItemByName(String name);
//
//    @Query(value="{category:'?0'}", fields="{'name' : 1, 'quantity' : 1}")
//    List<User> findAll(String category);

//    public long count();

}