package at.jku.dke.dwh.enron;

import org.apache.commons.io.FileUtils;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

public class EmailJob implements Job {
	private final Logger logger = LoggerFactory.getLogger(EmailJob.class);

	String[] emails = {
			"schuetz@dke.uni-linz.ac.at",
			"schrefl@dke.uni-linz.ac.at",
			"neumayr@dke.uni-linz.ac.at",
			"strasser@dke.uni-linz.ac.at",
			"staudinger@dke.uni-linz.ac.at"
	};

	String[] words = {
			"Hallo", "wie", "geht", "es", "dir", "mir",
			"gut", "Danke", "sehr"
	};

	@Override
	public void execute(JobExecutionContext context) W{
		JobDataMap data = context.getJobDetail().getJobDataMap();

		File outputDirectory = (File) data.get("outputDirectory");
		int batchSize = (int) data.get("batchSize");

	    Random randomGenerator = new Random();

		File destinationFile;
		try {
			// create a temp file to write the e-mail input into
			destinationFile = File.createTempFile("email-", ".tmp", outputDirectory);

			try(FileWriter writer = new FileWriter(destinationFile)) {
				for (int i = 1; i <= batchSize; i++) {
					StringBuilder bodyBuilder = new StringBuilder();
					for(int j = 0; j <= randomGenerator.nextInt(20); j++) {
						bodyBuilder.append(words[randomGenerator.nextInt(words.length)]).append(" ");
					}

					String line =
							emails[randomGenerator.nextInt(emails.length)] + "," +
							emails[randomGenerator.nextInt(emails.length)] + "," +
							LocalDateTime.now().plusSeconds(randomGenerator.nextInt(10)).format(DateTimeFormatter.ISO_DATE_TIME)  + "," +
							bodyBuilder.toString() + "\n";
					writer.write(line);
				}
			}

			logger.info("Set last modified timestamp to current time");
		    boolean successful = destinationFile.setLastModified(System.currentTimeMillis());
		} catch (IOException e) {
			logger.error("Couldn't write file", e);
		}
	}
	
}
