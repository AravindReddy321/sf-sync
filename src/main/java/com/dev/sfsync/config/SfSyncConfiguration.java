package com.dev.sfsync.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.client.RestClient;

@Configuration
public class SfSyncConfiguration {

    @Bean
    public RestClient restClient(){
        return RestClient.builder().build();
    }

    @Bean
    public SecurityFilterChain httpFilterChain(HttpSecurity http){
        return http.
                authorizeHttpRequests(matchRegister -> matchRegister
                        .anyRequest().permitAll())
                .formLogin(Customizer.withDefaults())
                .csrf(httpSecurityCsrfConfigurer -> httpSecurityCsrfConfigurer.disable())
                .headers(headers-> headers
                        .frameOptions(frameOptions -> frameOptions.sameOrigin()))
                .build();
    }
}
