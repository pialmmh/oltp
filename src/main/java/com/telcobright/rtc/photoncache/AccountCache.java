package com.telcobright.rtc.photoncache;

import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.PreDestroy;
import java.io.File;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.*;

public class AccountCache {
    private static final Logger logger = LogManager.getLogger(AccountCache.class);
    private final AccountStorage storage;
    private final AccountDeltaQueue deltaQueue;
    private final AccountDeltaConsumer consumer;
    private final ExecutorService executor;

    public AccountCache() {
        this.storage = new AccountStorage();
        this.deltaQueue = new AccountDeltaQueue("accountQueue", 7);
        this.consumer = new AccountDeltaConsumer(deltaQueue.createTailer(), storage);
        this.executor = Executors.newSingleThreadExecutor();
    }

    public void initializeAccounts(Map<Integer, Account> initialAccounts) {
        storage.initialize(initialAccounts);
        logger.info("Initialized accounts: {}", initialAccounts.keySet());
        consumer.replayPendingDeltas();
        executor.submit(consumer);
    }

    public void updateAccount(Account delta) {
        try {
            if (!storage.contains(delta.getId())) {
                logger.warn("Ignoring delta update: account {} not initialized", delta.getId());
                return;
            }
            ExcerptAppender appender = deltaQueue.getAppender();
            appender.writeDocument(w -> {
                w.write("id").int32(delta.getId());
                w.write("delta").marshallable(delta);
            });
            storage.applyDelta(delta.getId(), delta);
        } catch (Exception e) {
            logger.error("Failed to update queue for account {}", delta.getId(), e);
        }
    }

    public Account getAccount(int id) {
        return storage.get(id);
    }

    @PreDestroy
    public void shutdown() {
        consumer.stop();
        executor.shutdown();
        deltaQueue.close();
    }
}






