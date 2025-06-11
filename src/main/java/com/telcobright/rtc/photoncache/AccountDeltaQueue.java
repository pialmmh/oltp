package com.telcobright.rtc.photoncache;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Files;


class AccountDeltaQueue {
    private static final Logger logger = LogManager.getLogger(AccountDeltaQueue.class);
    private final ChronicleQueue queue;
    private final ThreadLocal<ExcerptAppender> threadLocalAppender;

    public AccountDeltaQueue(String queuePath, int retentionDays) {
        this.queue = ChronicleQueue.singleBuilder(queuePath)
                .rollCycle(RollCycles.FAST_DAILY)
                .storeFileListener(new StoreFileListener() {
                    @Override
                    public void onAcquired(int cycle, File file) {
                        cleanOldFiles(file.getParentFile(), cycle, retentionDays);
                    }

                    @Override
                    public void onReleased(int cycle, File file) {}

                    private void cleanOldFiles(File queueDir, int currentCycle, int retentionDays) {
                        long oldestCycleToKeep = currentCycle - retentionDays;
                        File[] files = queueDir.listFiles((dir, name) -> name.endsWith(".cq4"));
                        if (files != null) {
                            for (File file : files) {
                                try {
                                    String baseName = file.getName().split("\\.")[0];
                                    String numericPart = baseName.replaceAll("\\D", "");
                                    if (!numericPart.isEmpty()) {
                                        long fileCycle = Long.parseLong(numericPart);
                                        if (fileCycle < oldestCycleToKeep) {
                                            Files.delete(file.toPath());
                                            logger.info("Deleted old queue file: {}", file.getName());
                                        }
                                    }
                                } catch (Exception e) {
                                    logger.warn("Could not process file: {}", file.getName());
                                }
                            }
                        }
                    }
                })
                .build();

        this.threadLocalAppender = ThreadLocal.withInitial(queue::createAppender);
    }

    public ExcerptAppender getAppender() {
        return threadLocalAppender.get();
    }

    public ExcerptTailer createTailer() {
        return queue.createTailer();
    }

    public void close() {
        queue.close();
    }
}