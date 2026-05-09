package com.techbleat.bank.service;

import com.techbleat.bank.model.Account;
import com.techbleat.bank.model.BankTransaction;
import com.techbleat.bank.repo.AccountRepository;
import com.techbleat.bank.repo.BankTransactionRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge; // Added for Active Sessions
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct; // Added for Metric Initialization
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled; // Added for 5-min cleanup
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
public class TransactionService {

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private BankTransactionRepository transactionRepository;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private MeterRegistry meterRegistry;

    // Tracker for unique active users in the last 5 minutes
    private final Set<String> activeUsers = ConcurrentHashMap.newKeySet();

    private final String redisHost = System.getenv().getOrDefault("REDIS_HOST", "redis");
    private final int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));

    /**
     * Initializes the Active Sessions Gauge.
     */
    @PostConstruct
    public void init() {
        Gauge.builder("banking_active_sessions", activeUsers, Set::size)
                .description("Users with activity in the last 5 minutes")
                .register(meterRegistry);
    }

    /**
     * Clears the active session set every 5 minutes to meet the requirement.
     */
    @Scheduled(fixedRate = 300000) // 300,000ms = 5 minutes
    public void clearActiveSessions() {
        activeUsers.clear();
    }

    public Map<String, Object> deposit(String userId, Double amount) {
        long startTime = System.nanoTime();
        activeUsers.add(userId); // Track activity
        try {
            validateAmount(amount);
            Account account = getAccount(userId);

            account.setBalance(account.getBalance() + amount);
            accountRepository.save(account);

            saveTransaction(userId, "DEPOSIT", amount, "cash deposit");
            publishEvent(userId, "DEPOSIT", amount);
            cacheBalance(userId, account.getBalance());
            
            recordTransactionMetrics("DEPOSIT", amount, "SUCCESS");
            recordLatency("deposit", startTime);

            return Map.of("message", "Deposit successful", "balance", account.getBalance());
        } catch (Exception e) {
            recordTransactionMetrics("DEPOSIT", amount, "FAILURE");
            throw e;
        }
    }

    public Map<String, Object> withdraw(String userId, Double amount) {
        long startTime = System.nanoTime();
        activeUsers.add(userId); // Track activity
        try {
            validateAmount(amount);
            Account account = getAccount(userId);

            if (account.getBalance() < amount) {
                recordTransactionMetrics("WITHDRAW", amount, "FAILURE");
                throw new RuntimeException("Insufficient funds");
            }

            account.setBalance(account.getBalance() - amount);
            accountRepository.save(account);

            saveTransaction(userId, "WITHDRAW", amount, "cash withdrawal");
            publishEvent(userId, "WITHDRAW", amount);
            cacheBalance(userId, account.getBalance());
            
            recordTransactionMetrics("WITHDRAW", amount, "SUCCESS");
            recordLatency("withdraw", startTime);

            return Map.of("message", "Withdrawal successful", "balance", account.getBalance());
        } catch (Exception e) {
            if (!(e instanceof RuntimeException && e.getMessage().equals("Insufficient funds"))) {
                 recordTransactionMetrics("WITHDRAW", amount, "FAILURE");
            }
            throw e;
        }
    }

    public Map<String, Object> transfer(String fromUserId, String toUserId, Double amount, String reference) {
        long startTime = System.nanoTime();
        activeUsers.add(fromUserId); // Track activity
        try {
            validateAmount(amount);

            Account fromAccount = getAccount(fromUserId);
            Account toAccount = getAccount(toUserId);

            if (fromAccount.getBalance() < amount) {
                recordTransactionMetrics("TRANSFER", amount, "FAILURE");
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
            
            recordTransactionMetrics("TRANSFER", amount, "SUCCESS");
            recordLatency("transfer", startTime);

            return Map.of(
                    "message", "Transfer successful",
                    "fromBalance", fromAccount.getBalance(),
                    "toBalance", toAccount.getBalance()
            );
        } catch (Exception e) {
            recordTransactionMetrics("TRANSFER", amount, "FAILURE");
            throw e;
        }
    }

    private void recordTransactionMetrics(String type, Double amount, String status) {
        Counter.builder("banking_transactions_total")
                .tag("type", type)
                .tag("status", status)
                .register(meterRegistry)
                .increment();

        if ("SUCCESS".equals(status) && amount != null) {
            Counter.builder("banking_transaction_value_total")
                    .tag("type", type)
                    .register(meterRegistry)
                    .increment(amount);
        }
    }

    private void recordLatency(String operation, long startTime) {
        long duration = System.nanoTime() - startTime;
        Timer.builder("banking_transaction_duration_seconds")
                .tag("operation", operation)
                .register(meterRegistry)
                .record(duration, TimeUnit.NANOSECONDS);
    }

    public Double getBalance(String userId) {
        activeUsers.add(userId); // Even checking balance counts as activity
        try (Jedis jedis = new Jedis(redisHost, redisPort)) {
            String value = jedis.get("balance:" + userId);
            if (value != null) {
                return Double.parseDouble(value);
            }
        } catch (Exception ignored) {}

        Account account = getAccount(userId);
        cacheBalance(userId, account.getBalance());
        return account.getBalance();
    }

    public List<BankTransaction> getTransactions(String userId) {
        activeUsers.add(userId); // View statement counts as activity
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
        } catch (Exception ignored) {}
    }
}