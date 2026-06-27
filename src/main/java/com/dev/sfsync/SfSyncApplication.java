package com.dev.sfsync;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SfSyncApplication {

    public static void main(String[] args) {
        SpringApplication.run(SfSyncApplication.class, args);
    }

}
