package com.workze.util.kafka.handler;

/**
 * @author wangguize
 * date 2019-09-13
 */
@FunctionalInterface
public interface KeyValueHandler<K,V> {
    void handle(K key, V value);
}
