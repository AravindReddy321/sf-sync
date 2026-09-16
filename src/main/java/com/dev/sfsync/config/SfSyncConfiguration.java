package com.dev.sfsync.config;

import com.dev.sfsync.client.SfSyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.client.RestClient;

@Configuration
public class SfSyncConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(SfSyncConfiguration.class);

    @Bean
    public String sfUriBean(@Value("${sf.uri}") String sfUri){
        return sfUri;
    }

    @Bean
    public String clientIdBean(@Value("${sf.clientid}") String clientId){
        return clientId;
    }

    @Bean
    public String clientSecret(@Value("${sf.clientsecret}") String clientSecret){
        return clientSecret;
    }

    @Bean
    public String sfOrgIdBean(@Value("${sf.tenantid}") String sfOrgId){
        return sfOrgId;
    }

    @Bean
    public String accessTokenBean(SfSyncClient sfSyncClient){
        return  sfSyncClient.getAccessToken();
    }

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
