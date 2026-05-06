package com.techbleat.bank.service;

import com.techbleat.bank.model.Account;
import com.techbleat.bank.model.BankTransaction;
import com.techbleat.bank.repo.AccountRepository;
import com.techbleat.bank.repo.BankTransactionRepository;
import io.micrometer.core.instrument.Counter; // Added for Metrics
import io.micrometer.core.instrument.MeterRegistry; // Added for Metrics
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class TransactionService {

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private BankTransactionRepository transactionRepository;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private MeterRegistry meterRegistry; // Added for Metrics

    private final String redisHost = System.getenv().getOrDefault("REDIS_HOST", "redis");
    private final int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));

    public Map<String, Object> deposit(String userId, Double amount) {
        validateAmount(amount);
        Account account = getAccount(userId);

        account.setBalance(account.getBalance() + amount);
        accountRepository.save(account);

        saveTransaction(userId, "DEPOSIT", amount, "cash deposit");
        publishEvent(userId, "DEPOSIT", amount);
        cacheBalance(userId, account.getBalance());
        
        // Metrics: Increment the transaction counter
        recordTransactionMetric("DEPOSIT");

        return Map.of("message", "Deposit successful", "balance", account.getBalance());
    }

    public Map<String, Object> withdraw(String userId, Double amount) {
        validateAmount(amount);
        Account account = getAccount(userId);

        if (account.getBalance() < amount) {
            throw new RuntimeException("Insufficient funds");
        }

        account.setBalance(account.getBalance() - amount);
        accountRepository.save(account);

        saveTransaction(userId, "WITHDRAW", amount, "cash withdrawal");
        publishEvent(userId, "WITHDRAW", amount);
        cacheBalance(userId, account.getBalance());
        
        // Metrics: Increment the transaction counter
        recordTransactionMetric("WITHDRAW");

        return Map.of("message", "Withdrawal successful", "balance", account.getBalance());
    }

    public Map<String, Object> transfer(String fromUserId, String toUserId, Double amount, String reference) {
        validateAmount(amount);

        Account fromAccount = getAccount(fromUserId);
        Account toAccount = getAccount(toUserId);

        if (fromAccount.getBalance() < amount) {
            throw new RuntimeException("Insufficient funds");
        }

        fromAccount.setBalance(fromAccount.getBalance() - amount);
        toAccount.setBalance(toAccount.getBalance() + amount);

        accountRepository.save(fromAccount);
        accountRepository.save(toAccount);

        saveTransaction(fromUserId, "TRANSFER_OUT", amount, reference == null || reference.isBlank() ? "to:" + toUserId : reference);
        saveTransaction(toUserId, "TRANSFER_IN", amount, reference == null || reference.isBlank() ? "from:" + fromUserId : reference);

        publishEvent(fromUserId, "TRANSFER_OUT", amount);
        publishEvent(toUserId, "TRANSFER_IN", amount);

        cacheBalance(fromUserId, fromAccount.getBalance());
        cacheBalance(toUserId, toAccount.getBalance());
        
        // Metrics: Increment the transaction counter
        recordTransactionMetric("TRANSFER");

        return Map.of(
                "message", "Transfer successful",
                "fromBalance", fromAccount.getBalance(),
                "toBalance", toAccount.getBalance()
        );
    }

    // New helper method for Micrometer Metrics
    private void recordTransactionMetric(String type) {
        Counter.builder("banking_transactions_total")
               .tag("type", type)
               .description("Total number of banking transactions")
               .register(meterRegistry)
               .increment();
    }

    public Double getBalance(String userId) {
        try (Jedis jedis = new Jedis(redisHost, redisPort)) {
            String value = jedis.get("balance:" + userId);
            if (value != null) {
                return Double.parseDouble(value);
            }
        } catch (Exception ignored) {
        }

        Account account = getAccount(userId);
        cacheBalance(userId, account.getBalance());
        return account.getBalance();
    }

    public List<BankTransaction> getTransactions(String userId) {
        return transactionRepository.findByUserIdOrderByCreatedAtDesc(userId);
    }

    private Account getAccount(String userId) {
        return accountRepository.findById(userId)
                .orElseThrow(() -> new RuntimeException("Account not found"));
    }

    private void validateAmount(Double amount) {
        if (amount == null || amount <= 0) {
            throw new RuntimeException("Amount must be greater than zero");
        }
    }

    private void saveTransaction(String userId, String type, Double amount, String reference) {
        BankTransaction tx = new BankTransaction();
        tx.setUserId(userId);
        tx.setTransactionType(type);
        tx.setAmount(amount);
        tx.setReference(reference);
        transactionRepository.save(tx);
    }

    private void publishEvent(String userId, String eventType, Double amount) {
        Map<String, Object> event = new HashMap<>();
        event.put("userId", userId);
        event.put("eventType", eventType);
        event.put("amount", amount);
        event.put("timestamp", Instant.now().toString());

        kafkaTemplate.send("banking-transactions", userId, event);
    }

    private void cacheBalance(String userId, Double balance) {
        try (Jedis jedis = new Jedis(redisHost, redisPort)) {
            jedis.set("balance:" + userId, String.valueOf(balance));
        } catch (Exception ignored) {
        }
    }
}