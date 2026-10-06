package com.techbleat.bank.controller;

import com.techbleat.bank.config.AccessGuard;
import com.techbleat.bank.dto.AmountRequest;
import com.techbleat.bank.dto.TransferRequest;
import com.techbleat.bank.service.TransactionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/")
public class TransactionController {

    @Autowired
    private TransactionService transactionService;

    @Autowired
    private AccessGuard accessGuard;

    @GetMapping("/health")
    public Object health() {
        return java.util.Map.of("status", "ok");
    }

    @PostMapping(value = "/transactions/deposit", consumes = {"application/json", "application/x-www-form-urlencoded"})
    public Object deposit(@RequestHeader("X-User-Id") String userId, @RequestBody AmountRequest request) {
        accessGuard.requireAccess(userId);
        return transactionService.deposit(userId, request.getAmount());
    }

    @PostMapping(value = "/transactions/withdraw", consumes = {"application/json", "application/x-www-form-urlencoded"})
    public Object withdraw(@RequestHeader("X-User-Id") String userId, @RequestBody AmountRequest request) {
        accessGuard.requireAccess(userId);
        return transactionService.withdraw(userId, request.getAmount());
    }

    @PostMapping(value = "/transactions/transfer", consumes = {"application/json", "application/x-www-form-urlencoded"})
    public Object transfer(@RequestHeader("X-User-Id") String userId, @RequestBody TransferRequest request) {
        accessGuard.requireAccess(userId);
        return transactionService.transfer(userId, request.getToUserId(), request.getAmount(), request.getReference());
    }

    @GetMapping("/balance/{userId}")
    public Object balance(@PathVariable String userId) {
        accessGuard.requireAccess(userId);
        return java.util.Map.of("userId", userId, "balance", transactionService.getBalance(userId));
    }

    @GetMapping("/transactions/{userId}")
    public Object transactions(@PathVariable String userId) {
        accessGuard.requireAccess(userId);
        return transactionService.getTransactions(userId);
    }
}
