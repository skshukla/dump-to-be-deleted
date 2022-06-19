package com.example.sachin.myDebezium.services.demos;

import com.example.sachin.myDebezium.services.AbstractDemoService;
import com.example.sachin.myDebezium.util.StreamUtil;
import com.example.sachin.myDebezium.vo.Address;
import com.example.sachin.myDebezium.vo.User;
import com.example.sachin.myDebezium.vo.UserAddressJoin;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Service;

@Service
public class MultipleResourcesUpdatesInSingleTxn extends AbstractDemoService {

    @Override
    protected void doExecute(final StreamsBuilder streamsBuilder) {
        final KTable<String, User> ktUser = this.getUserAsKTable(streamsBuilder);
        final KStream<String, Address> ksAddress = this.getAddressAsStream(streamsBuilder)
                .selectKey( (k, v) -> String.valueOf(v.getUserId()));

        final KGroupedStream<String, Address> ksAddressGrouped = ksAddress.groupByKey(Grouped.with(Serdes.String(), StreamUtil.getSerde(new Address())));

        final Materialized<String, UserAddressJoin, KeyValueStore<Bytes, byte[]>> mat =
                Materialized.<String, UserAddressJoin, KeyValueStore<Bytes, byte[]>>as("bbbb")
                        .withValueSerde(StreamUtil.getSerde(new UserAddressJoin()));

        final KTable<String, UserAddressJoin> kt = ksAddressGrouped.aggregate( () -> new UserAddressJoin(), (k, v, a) -> {
            a.setId(Integer.parseInt(k));
            a.addAddress(v);
            return a;
        }, mat);


        final Materialized<String, UserAddressJoin, KeyValueStore<Bytes, byte[]>> mat2 =
                Materialized.<String, UserAddressJoin, KeyValueStore<Bytes, byte[]>>as("cccc")
                        .withValueSerde(StreamUtil.getSerde(new UserAddressJoin()));

        final KTable<String, UserAddressJoin> ktUAJEnriched = kt.join(ktUser, (uaj, u)-> {
            uaj.setFirstName(u.getFirstName());
            uaj.setLastName(u.getLastName());
            uaj.setEmail(u.getEmail());
            return uaj;
        }, mat2);

        StreamUtil.printStream("summarized_user_address_stream", ktUAJEnriched.toStream());
    }
}
