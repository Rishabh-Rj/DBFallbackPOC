package net.javaguides.ems.config;

import net.javaguides.ems.event.EmployeeEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;


@Configuration
public class KafkaConsumerConfig {
@Bean
public ConcurrentKafkaListenerContainerFactory<String, EmployeeEvent> kafkaListenerContainerFactory(
        ConsumerFactory<String, EmployeeEvent> consumerFactory,DefaultErrorHandler kafkaErrorHandler) {
    ConcurrentKafkaListenerContainerFactory<String, EmployeeEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL); // your manual ack
//    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
//    factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(5000L, FixedBackOff.UNLIMITED_ATTEMPTS)));
    factory.setCommonErrorHandler(kafkaErrorHandler);
    return factory;
}}