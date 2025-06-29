package net.javaguides.ems.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class DataSourceConfig {
    @Bean
    @Primary
    public DataSource routingDataSource(
            @Qualifier("mysqlDataSource") DataSource mysqlDataSource,
            @Qualifier("h2DataSource") DataSource h2DataSource) {
        Map<Object, Object> targetDataSources = new HashMap<>();
        targetDataSources.put("mysql", mysqlDataSource);
        targetDataSources.put("h2", h2DataSource);

        RoutingDataSource routingDataSource = new RoutingDataSource();
        routingDataSource.setTargetDataSources(targetDataSources);
        routingDataSource.setDefaultTargetDataSource(mysqlDataSource);
        return routingDataSource;
    }

    @Bean("mysqlDataSource")
    public DataSource mysqlDataSource(Environment env) {
        return DataSourceBuilder.create()
                .url(env.getProperty("app.datasource.mysql.url"))
                .username(env.getProperty("app.datasource.mysql.username"))
                .password(env.getProperty("app.datasource.mysql.password"))
                .driverClassName(env.getProperty("app.datasource.mysql.driver-class-name"))
                .build();
    }

    @Bean("h2DataSource")
    public DataSource h2DataSource(Environment env) {
        return DataSourceBuilder.create()
                .url(env.getProperty("app.datasource.h2.url"))
                .username(env.getProperty("app.datasource.h2.username"))
                .password(env.getProperty("app.datasource.h2.password"))
                .driverClassName(env.getProperty("app.datasource.h2.driver-class-name"))
                .build();
    }
}
