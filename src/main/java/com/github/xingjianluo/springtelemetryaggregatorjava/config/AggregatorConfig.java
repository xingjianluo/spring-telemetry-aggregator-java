package com.github.xingjianluo.springtelemetryaggregatorjava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(AggregatorProperties.class)
public class AggregatorConfig {
}
