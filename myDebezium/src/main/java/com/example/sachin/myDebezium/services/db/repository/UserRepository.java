package com.example.sachin.myDebezium.services.db.repository;

import com.example.sachin.myDebezium.entity.User;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepository extends CrudRepository<User, Long> {

}
