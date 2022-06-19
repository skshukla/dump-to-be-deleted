package com.example.sachin.myDebezium.services;

import com.example.sachin.myDebezium.util.StreamUtil;
import com.example.sachin.myDebezium.vo.User;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.stereotype.Service;

@Service
public class MySampleStreamApp extends AbstractDemoService{

    @Override
    protected void doExecute(final StreamsBuilder streamsBuilder) {
        final KTable<String, User> sUser =  this.getUserAsKTable(streamsBuilder);
        StreamUtil.printStream("sac", sUser.toStream());
    }
}
