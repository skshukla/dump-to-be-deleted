package com.example.sachin.myDebezium.services.db;

import com.example.sachin.myDebezium.entity.User;
import com.example.sachin.myDebezium.services.db.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class UserService {

    @Autowired
    private UserRepository userRepository;

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true, rollbackFor = Exception.class)
    public User findById(final long userId) {
        final Optional<User> userOptional = this.userRepository.findById(userId);
        return userOptional.isPresent() ? userOptional.get() : new User();
    }
}
