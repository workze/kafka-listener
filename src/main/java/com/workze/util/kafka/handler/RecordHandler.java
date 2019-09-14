package com.workze.util.kafka.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author wangguize
 * date 2019-09-13
 */
@FunctionalInterface
public interface RecordHandler<K, V> {
    void handle(ConsumerRecord<K, V> record);
}
