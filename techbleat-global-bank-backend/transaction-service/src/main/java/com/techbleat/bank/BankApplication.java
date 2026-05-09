package com.techbleat.bank;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling; // Added for 5-min session cleanup

@SpringBootApplication
@EnableScheduling // Added to enable the @Scheduled task in TransactionService
public class BankApplication {
    public static void main(String[] args) {
        SpringApplication.run(BankApplication.class, args);
    }
}