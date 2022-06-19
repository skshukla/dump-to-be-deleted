package com.example.sachin.myDebezium.services;

import com.example.sachin.myDebezium.util.Constants;
import com.example.sachin.myDebezium.util.StreamUtil;
import com.example.sachin.myDebezium.vo.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.Set;

@Service
@Slf4j
public class StateStoreInStreamsDemo extends AbstractDemoService {


    @Override
    protected void doExecute(final StreamsBuilder streamsBuilder) {
        log.info("streamJoins_cacheMultipleKTables(): Entering method {run()}, with USER_TOPIC val as {{}}, USER_ADDRESS_TOPIC val as {{}}", USER_TOPIC, USER_ADDRESS_TOPIC);
        final KStream<Integer, User> sUser = this.getUserAsStream(streamsBuilder);

        final ValueTransformerSupplier<User, Integer> o = new ValueTransformerSupplier<User, Integer>() {
            @Override
            public Set<StoreBuilder<?>> stores() {
                return ValueTransformerSupplier.super.stores();
            }

            @Override
            public ValueTransformer<User, Integer> get() {
                return new NumOfUpdatesTransformer(Constants.STORE_NAME_DEMO);
            }
        };
        final KStream<Integer, Integer> counterStream = sUser.transformValues(o, Constants.STORE_NAME_DEMO);
        StreamUtil.printStream("result-counter", counterStream);

    }

    class NumOfUpdatesTransformer implements ValueTransformer<User, Integer> {

        private ProcessorContext processorContext;
        private String  storeName;
        private KeyValueStore<Integer, Integer> stateStore;

        public NumOfUpdatesTransformer(final String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(final ProcessorContext o) {
            this.processorContext = o;
            this.stateStore = this.processorContext.getStateStore(Constants.STORE_NAME_DEMO);
        }

        @Override
        public Integer transform(final User u) {
            int n = Objects.isNull(this.stateStore.get(u.getId())) ? 0 : this.stateStore.get(u.getId());
            this.stateStore.put(u.getId(), 1 + n);
            return 1+n;
        }

        @Override
        public void close() {

        }
    }
}
