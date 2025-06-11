package com.telcobright.rtc.photoncache;

import net.openhft.chronicle.queue.ExcerptTailer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class AccountDeltaConsumer implements Runnable {
    private static final Logger logger = LogManager.getLogger(AccountDeltaConsumer.class);
    private final ExcerptTailer tailer;
    private final AccountStorage storage;
    private volatile boolean running = true;

    public AccountDeltaConsumer(ExcerptTailer tailer, AccountStorage storage) {
        this.tailer = tailer;
        this.storage = storage;
    }

    public void stop() {
        this.running = false;
    }

    public void replayPendingDeltas() {
        while (true) {
            boolean read = tailer.readDocument(w -> {
                int id = w.read("id").int32();
                Account delta = new Account();
                w.read("delta").marshallable(delta);
                if (storage.contains(id)) {
                    storage.applyDelta(id, delta);
                    logger.info("Applied pending delta to account {}: {}", id, delta);
                } else {
                    logger.warn("Skipping delta for unknown account ID {} during initialization", id);
                }
            });
            if (!read) break;
        }
    }

    @Override
    public void run() {
        while (running) {
            try {
                boolean read = tailer.readDocument(w -> {
                    int id = w.read("id").int32();
                    Account delta = new Account();
                    w.read("delta").marshallable(delta);
                    if (storage.contains(id)) {
                        storage.applyDelta(id, delta);
                        logger.info("Updated in DB: {}", storage.get(id));
                    } else {
                        logger.warn("Skipping delta for unknown account ID {} in consume loop", id);
                    }
                });
                if (!read) {
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                logger.error("Failed to process message", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}