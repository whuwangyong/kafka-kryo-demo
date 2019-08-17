package com.example.demo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


@SpringBootApplication
public class Application implements CommandLineRunner {

    public static Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args).close();
    }

    @Autowired
    private KafkaTemplate<String, byte[]> template;

    private final CountDownLatch latch = new CountDownLatch(3);

    String topic = "testWY";

    @Override
    public void run(String... args) throws Exception {

        Person wangyong = Person.builder().name("wangyong").age(28).build();


        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeClassAndObject(output, wangyong);
        output.close();

        byte[] bytes = baos.toByteArray();


        ProducerRecord<String, byte[]> msgtar = new ProducerRecord<>(topic, "LL2274082JW100128", bytes);
        template.send(msgtar);
        logger.info("sent done");
        latch.await(60, TimeUnit.SECONDS);
        logger.info("All received");
    }

    @KafkaListener(topics = "testWY")
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {

        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        ByteArrayInputStream bais = new ByteArrayInputStream((byte[]) cr.value());
        Input input = new Input(bais);
        Person wangyong = (Person) kryo.readClassAndObject(input);
        input.close();

        logger.info("name:" + wangyong.name);
        logger.info("age:" + wangyong.age);
        latch.countDown();
    }

}