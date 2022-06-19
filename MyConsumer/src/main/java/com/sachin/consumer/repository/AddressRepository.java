package com.sachin.consumer.repository;


import com.sachin.consumer.entity.Address;
import com.sachin.consumer.entity.User;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AddressRepository extends CrudRepository<Address, Long> {

}
