package com.Data.synchronization.configuration;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Bean
    @Qualifier("mysqlDataSource")
    public DataSource mysqlDataSource(@Value("${spring.datasource.mysql.url}") String url,
                                      @Value("${spring.datasource.mysql.username}") String username,
                                      @Value("${spring.datasource.mysql.password}") String password) {
        return DataSourceBuilder.create()
                .url(url)
                .username(username)
                .password(password)
                .build();
    }

    @Bean
    @Qualifier("postgresDataSource")
    public DataSource postgresDataSource(@Value("${spring.datasource.postgres.url}") String url,
                                         @Value("${spring.datasource.postgres.username}") String username,
                                         @Value("${spring.datasource.postgres.password}") String password) {
        return DataSourceBuilder.create()
                .url(url)
                .username(username)
                .password(password)
                .build();
    }
}

