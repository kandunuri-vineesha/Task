package com.Data.synchronization.batch;


import com.Data.synchronization.models.ChangeEvent;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

@Configuration
public class SyncJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public Job syncJob(Step syncStep) {
        return new JobBuilder("syncJob", jobRepository)
                .start(syncStep)
                .build();
    }

    @Bean
    public Step syncStep(ChangeEventReader reader,
                         ChangeEventProcessor processor,
                         ChangeEventWriter writer) {
        return new StepBuilder("syncStep", jobRepository)
                .<ChangeEvent, ChangeEvent>chunk(10, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .faultTolerant()
                .retry(Exception.class)
                .retryLimit(3)
                .build();
    }
}

@Component
public class ChangeEventReader implements ItemReader<ChangeEvent> {

    private final ChangeEventRepository repository;

    @Override
    public ChangeEvent read() {
        return repository.findFirstByStatusOrderByTimestamp("PENDING");
    }
}

@Component
public class ChangeEventProcessor implements ItemProcessor<ChangeEvent, ChangeEvent> {

    private final ConflictResolver conflictResolver;

    @Override
    public ChangeEvent process(ChangeEvent event) {

        return conflictResolver.resolve(event);
    }
}

@Component
public class ChangeEventWriter implements ItemWriter<ChangeEvent> {

    private final DatabaseAdapterFactory adapterFactory;

    @Override
    public void write(List<? extends ChangeEvent> events) {
        for (ChangeEvent event : events) {
            DatabaseAdapter adapter = adapterFactory.getAdapter(event.getTargetDb());
            adapter.applyChange(event);
            event.setStatus("COMPLETED");
        }
    }
}
