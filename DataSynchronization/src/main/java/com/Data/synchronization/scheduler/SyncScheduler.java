package com.Data.synchronization.scheduler;

import com.Data.synchronization.service.CDCService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SyncScheduler {

    private final JobLauncher jobLauncher;
    private final Job syncJob;
    private final CDCService cdcService;

    @Scheduled(fixedDelay = 5000)
    public void runSync() {
        try {
            JobParameters params = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .toJobParameters();

            jobLauncher.run(syncJob, params);
        } catch (Exception e) {
            log.error("Error running sync job", e);
        }
    }

    @Scheduled(fixedDelay = 1000)
    public void captureChanges() {
        databaseConfigService.getActiveConfigs()
                .forEach(config -> cdcService.captureChanges(config.getName()));
    }
}

