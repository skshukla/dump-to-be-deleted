package com.example.sachin.myDebezium;

import com.example.sachin.myDebezium.entity.User;
import com.example.sachin.myDebezium.services.db.UserService;
import com.example.sachin.myDebezium.services.demos.MultipleResourcesUpdatesInSingleTxn;
import com.example.sachin.myDebezium.services.demos.WindowedStream;
import com.example.sachin.myDebezium.services.demos.WindowedStreamSummarized;
import com.example.sachin.myDebezium.services.demos.WindowedStreamSummarized3;
import com.example.sachin.myDebezium.util.GenUtil;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serial;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;


@SpringBootApplication
@EnableKafka
@Slf4j
public class MyDebeziumApplication implements CommandLineRunner {

    @Autowired
    private ApplicationContext applicationContext;

    protected Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    private Gson GSON_TO_PRINT = new GsonBuilder()
            .setPrettyPrinting()
            .create();


    @Value("${topics.user}")
    private String USER_TOPIC;

    @Value("${topics.user-address}")
    private String USER_ADDRESS_TOPIC;

    public static void main(String[] args) {
        SpringApplication.run(MyDebeziumApplication.class, args);
    }

    @Autowired
    private UserService userService;

    @Override
    public void run(String... args) throws Exception {
        this.runMe();

//        Executors.newSingleThreadScheduledExecutor().execute( () -> {
//            log.info("obj2323 {{}}", this.userService.findById(2));
//        });

//        this.runSummarizedFlow();
//        this.runSummarizedFlow3();
//        this.applicationContext.getBean(MyTestStreamingApp.class).run();
//        this.applicationContext.getBean(WindowedStream.class).run();
//        this.applicationContext.getBean(MultipleResourcesUpdatesInSingleTxn.class).run();
//        this.applicationContext.getBean(WithAndWithoutLoggingKStreams.class).run();
//        this.applicationContext.getBean(MySampleStreamApp.class).run();
//        this.applicationContext.getBean(StateStoreInStreamsDemo.class).run();
    }


    private void runMe() throws Exception {
        log.info("Inside runMe()......");
        final String path = "/Users/sachin/work/workspaces/ws/myDebezium/test3.sql";
        final String insertStatement = "insert into t01 (\n" +
                "col_001, col_002, col_003, col_004, col_005, col_006, col_007, col_008, col_009, col_010, col_011, col_012, col_013, col_014, col_015, col_016, col_017, col_018, col_019, col_020, col_021, col_022, col_023, col_024, col_025, col_026, col_027, col_028, col_029, col_030, col_031, col_032, col_033, col_034, col_035, col_036, col_037, col_038, col_039, col_040, col_041, col_042, col_043, col_044, col_045, col_046, col_047, col_048, col_049, col_050, col_051, col_052, col_053, col_054, col_055, col_056, col_057, col_058, col_059, col_060, col_061, col_062, col_063, col_064, col_065, col_066, col_067, col_068, col_069, col_070, col_071, col_072, col_073, col_074, col_075, col_076, col_077, col_078, col_079, col_080, col_081, col_082, col_083, col_084, col_085, col_086, col_087, col_088, col_089, col_090, col_091, col_092, col_093, col_094, col_095, col_096, col_097, col_098, col_099, col_100, col_101, col_102, col_103, col_104, col_105, col_106, col_107, col_108, col_109, col_110, col_111, col_112, col_113, col_114, col_115, col_116, col_117, col_118, col_119, col_120, col_121, col_122, col_123, col_124, col_125, col_126, col_127, col_128, col_129, col_130, col_131, col_132, col_133, col_134, col_135, col_136, col_137, col_138, col_139, col_140, col_141, col_142, col_143, col_144, col_145, col_146, col_147, col_148, col_149, col_150, col_151, col_152, col_153, col_154, col_155, col_156, col_157, col_158, col_159, col_160, col_161, col_162, col_163, col_164, col_165, col_166, col_167, col_168, col_169, col_170, col_171, col_172, col_173, col_174, col_175, col_176, col_177, col_178, col_179, col_180, col_181, col_182, col_183, col_184, col_185, col_186, col_187, col_188, col_189, col_190, col_191, col_192, col_193, col_194, col_195, col_196, col_197, col_198, col_199, col_200, col_201, col_202, col_203, col_204, col_205, col_206, col_207, col_208, col_209, col_210, col_211, col_212, col_213, col_214, col_215, col_216, col_217, col_218, col_219, col_220, col_221, col_222, col_223, col_224, col_225, col_226, col_227, col_228, col_229, col_230, col_231, col_232, col_233, col_234, col_235, col_236, col_237, col_238, col_239, col_240, col_241, col_242, col_243, col_244, col_245, col_246, col_247, col_248, col_249, col_250\n" +
                ")\n" +
                "values \n";
        Files.write(Paths.get(path), insertStatement.getBytes());




        final int N_ROWS = 1000000;
        final int N_COLUMNS = 250;
        final StringBuilder sb = new StringBuilder();
        IntStream.range(0, N_ROWS).forEach(i -> {
            sb.append(this.prepareOneRow(N_COLUMNS));
            if (i < N_ROWS - 1) {
                sb.append(",");
            } else {
                sb.append(";");
            }
            sb.append("\n");

            if (i % 1000 == 0 && i != 0) {
                try {
                    Files.write(Paths.get(path), sb.toString().getBytes(), StandardOpenOption.APPEND);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    sb.setLength(0);
                }
            }

        });

//        log.info("\n\n" + sb.toString());
//        log.info("Going to write the content to file");
//        this.writeContentToFile(path, sb.toString());
//        log.info("Wrote the content to file");

    }



    private String prepareOneRow(int nColumns) {
        final StringBuilder sb = new StringBuilder();
        sb.append("(");
        IntStream.range(0, nColumns).forEach(i -> {
            sb.append("\'" + GenUtil.getRandomStringOfLength(100) +  "\'");
            if (i < nColumns - 1) {
                sb.append(",");
            }
        });
        sb.append(")");
        return sb.toString();
    }









    private void runSummarizedFlow() throws Exception {
        this.applicationContext.getBean(WindowedStream.class).run();
        Thread.currentThread().sleep(5 * 60 * 60 * 1000);
    }

    private void runSummarizedFlow3() throws Exception {
        this.applicationContext.getBean(WindowedStreamSummarized3.class).run();
        Thread.currentThread().sleep(5 * 60 * 60 * 1000);
    }


    @KafkaListener(topics = "my-topics-06-partitioned.public.user_tbl")
    public void listen(final ConsumerRecord<?, ?> record) {
        log.info("Record {{}}", record);
    }
}


