package com.sachin.consumer.service;


import com.sachin.consumer.entity.User;
import com.sachin.consumer.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

//import javax.transaction.Transactional;
import java.util.Optional;

@Service
public class UserService {

    @Autowired
    private UserRepository userRepository;

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true, rollbackFor = Exception.class)
//    @Transactional
    public User findById(final long userId) {
        this.userRepository.findById(userId);

        final Optional<User> userOptional = this.userRepository.findById(userId);
        return userOptional.isPresent() ? userOptional.get() : new User();
    }
}
