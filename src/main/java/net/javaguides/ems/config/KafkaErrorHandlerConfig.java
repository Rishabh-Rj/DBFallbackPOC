package net.javaguides.ems.config;



import net.javaguides.ems.service.impl.EmployeeServiceImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.kafka.KafkaException.Level;

@Configuration
public class KafkaErrorHandlerConfig {

    private static final Logger logger = LoggerFactory.getLogger("KafkaErrorHandler");

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        // Retry every 5 seconds indefinitely (can configure max retries if needed)
        FixedBackOff backOff = new FixedBackOff(5000L, FixedBackOff.UNLIMITED_ATTEMPTS);

        // lambda to log with SLF4J when error handler is triggered
        DefaultErrorHandler errorHandler = new DefaultErrorHandler((record, ex) -> {


            if (ex.getCause() instanceof EmployeeServiceImpl.SilentRetryException) {
                logger.warn("Silent retry — DB still down for record offset={}, partition={}", record.offset(), record.partition());
            } else {
                logger.warn("Kafka retry triggered for record offset={}, partition={}. Reason: {}", record.offset(), record.partition(), ex.getMessage());
            }
        }, backOff);



        errorHandler.setLogLevel(Level.WARN);

        return errorHandler;
    }
}


