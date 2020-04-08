package com.example.demo.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.example.demo.batch.DbWriter;
import com.example.demo.listerner.ChunkExecutionListener;
import com.example.demo.listerner.JobCompletionNotificationListener;
import com.example.demo.listerner.StepExecutionNotificationListener;
import com.example.demo.model.User;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {
	

	@Value("${chunk-size}")
	private int chunkSize;

	@Value("${max-threads}")
	private int maxThreads;
	
	@Autowired
    private JobBuilderFactory jobBuilderFactory;
     
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

	@Bean
	public DbWriter writer() {
		return new DbWriter();
	}

	@Bean
	public JobCompletionNotificationListener jobExecutionListener() {
		return new JobCompletionNotificationListener();
	}
	
	@Bean
	public StepExecutionNotificationListener stepExecutionListener() {
		return new StepExecutionNotificationListener();
	}
	
	@Bean
	public ChunkExecutionListener chunkListener() {
		return new ChunkExecutionListener();
	}

	@Bean
	public Job processAttemptJob() {
		return jobBuilderFactory.get("process-attempt-job")
				.incrementer(new RunIdIncrementer())
				.listener(jobExecutionListener())
				.flow(step()).end().build();
	}

	@Bean
	public Step step() {
		return stepBuilderFactory.get("step").<User, User>chunk(chunkSize)
				.reader(reader())
				.writer(writer())
				.taskExecutor(taskExecutor())
				.listener(stepExecutionListener())
				.listener(chunkListener())
				.throttleLimit(maxThreads).build();
	}
    
    
	@Bean
	public TaskExecutor taskExecutor() {
		SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
		taskExecutor.setConcurrencyLimit(maxThreads);
		return taskExecutor;
	}
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Bean
    public FlatFileItemReader<User> reader() 
    {
        //Create reader instance
        FlatFileItemReader<User> reader = new FlatFileItemReader<User>();
         
        //Set input file location
        reader.setResource(new FileSystemResource("input/inputData.csv"));
         
        //Set number of lines to skips. Use it if file has header rows.
        reader.setLinesToSkip(1);   
         
        //Configure how each line will be parsed and mapped to different values
        reader.setLineMapper(new DefaultLineMapper() {
            {
                //3 columns in each row
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames(new String[] { "id", "firstName", "lastName" });
                    }
                });
                //Set values in Employee class
                setFieldSetMapper(new BeanWrapperFieldSetMapper<User>() {
                    {
                        setTargetType(User.class);
                    }
                });
            }
        });
        return reader;
    }
     

}
