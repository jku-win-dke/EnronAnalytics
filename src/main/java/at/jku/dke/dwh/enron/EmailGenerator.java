package at.jku.dke.dwh.enron;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.repeatSecondlyForever;
import static org.quartz.TriggerBuilder.newTrigger;


public class EmailGenerator {
    private static final Logger logger = LoggerFactory.getLogger(EmailGenerator.class);
	
	public static void generate() {
		try {
			Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

			JobDetail job = newJob(EmailJob.class)
					.withIdentity("email", "emailGroup")
					.build();

			File  outputDirectory = new File("./output");
			Integer batchSize = 10;

			job.getJobDataMap().put("outputDirectory", outputDirectory);
			job.getJobDataMap().put("batchSize", batchSize);

			Trigger trigger = newTrigger()
					.withIdentity("emailTrigger", "emailTrigger")
					.startNow()
					.withSchedule(repeatSecondlyForever(10))
					.build();

			scheduler.scheduleJob(job, trigger);

			scheduler.start();
		} catch (SchedulerException e) {
			logger.error("Error in scheduler of e-mail generator", e);
		}
	}
	
}
