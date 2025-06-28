package net.javaguides.ems.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

import javax.sql.DataSource;

@Configuration
@EnableConfigurationProperties(DataSourceProperties.class)
public class DataSourceConfig {
    @Bean
    @ConditionalOnProperty(name = "app.datasource.primary", havingValue = "mysql", matchIfMissing = true)
    public DataSource mysqlDataSource(DataSourceProperties properties) {
        return DataSourceBuilder.create()
                .url(properties.getUrl())
                .username(properties.getUsername())
                .password(properties.getPassword())
                .driverClassName(properties.getDriverClassName())
                .build();
    }

    @Bean
    @ConditionalOnProperty(name = "app.datasource.primary", havingValue = "h2")
    public DataSource h2DataSource(DataSourceProperties properties) {
        return DataSourceBuilder.create()
                .url(properties.getUrl())
                .username(properties.getUsername())
                .password(properties.getPassword())
                .driverClassName(properties.getDriverClassName())
                .build();
    }
}
