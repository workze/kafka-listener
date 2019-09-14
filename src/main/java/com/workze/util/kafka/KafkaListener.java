package com.workze.util.kafka;

import com.workze.util.kafka.handler.KeyValueHandler;
import com.workze.util.kafka.handler.RecordHandler;
import com.workze.util.kafka.handler.ValueHandler;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * @author wangguize
 * date 2019-09-13
 */
@Getter
@Setter
@Accessors(chain = true, fluent = true)
@Slf4j
public class KafkaListener<K, V> {
    private Properties properties;
    private List<String> topics;
    private KeyValueHandler<K, V> keyValueHandler;
    private ValueHandler<V> valueHandler;
    private RecordHandler<K, V> recordHandler;

    private volatile boolean running = false;

    synchronized public void start() {
        if (!running) {
            validArgument();
            createPollThread();
            running = true;
        }
    }

    private void validArgument() {
        // topic
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("missing topic");
        }
        if (properties == null) {
            throw new IllegalArgumentException("missing properties");
        }
        if (keyValueHandler == null && valueHandler == null && recordHandler == null) {
            throw new IllegalArgumentException("missing message handler");
        }
    }

    private void createPollThread() {
        ExecutorService pollService = Executors.newSingleThreadExecutor();
        pollService.execute(() -> {
            KafkaConsumer<K, V> kafkaConsumer = buildKafkaConsumer();
            ExecutorService messageHandlerService = new ThreadPoolExecutor(1, 4, 60, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(10000),
                    new ThreadPoolExecutor.CallerRunsPolicy());

            while (running) {
                ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofDays(Integer.MAX_VALUE));
                for (ConsumerRecord<K, V> record : records) {
                    doHandler(messageHandlerService, record);
                }
            }

            kafkaConsumer.close();
            messageHandlerService.shutdown();
        });
        pollService.shutdown();
        log.info("create consumer-poll-thread success");
    }

    private void doHandler(ExecutorService messageHandlerService, ConsumerRecord<K, V> record) {
        log.debug("receive message, key is {}, value is {}", record.key(), record.value());
        messageHandlerService.execute(() -> {
            try {
                if (recordHandler != null) {
                    recordHandler.handle(record);
                    return;
                }
                if (keyValueHandler != null) {
                    keyValueHandler.handle(record.key(), record.value());
                    return;
                }
                if (valueHandler != null) {
                    valueHandler.handle(record.value());
                    return;
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });
    }

    private KafkaConsumer<K, V> buildKafkaConsumer() {
        KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(topics);
        return kafkaConsumer;
    }

    synchronized public void stop() {
        if (running) {
            running = false;
        }
    }

}

