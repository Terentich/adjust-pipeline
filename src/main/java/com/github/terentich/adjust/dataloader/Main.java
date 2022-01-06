package com.github.terentich.adjust.dataloader;

import com.github.terentich.adjust.dataloader.db.DbManager;
import com.github.terentich.adjust.dataloader.io.IgraFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final int MAX_PARALLEL_THREADS = 5;
    private static final Predicate<File> isArchivePredicate = (File file) ->
            file.getName().endsWith(".zip");
    private static Properties config;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Missing required input parameters:");
            System.err.println(Main.class.getSimpleName() + " <inputPath> <configFile>");
            System.err.println("inputPath - the path with archived files (zip) in the Igra format");
            System.err.println("configFile - the property file with the database settings");
        }

        Path inputPath = Paths.get(args[0]);
        String configFile = args[1];

        config = getProperties(configFile);
        DbManager dbManager = new DbManager(config);

        try (Connection ignored = dbManager.createDatabaseConnection();
             Stream<Path> paths = Files.list(inputPath)) {

            log.info("Creating database");
            dbManager.createDatabase();

            log.info("Start processing input path: {}", inputPath);

            List<File> files = paths
                    .filter(file -> !Files.isDirectory(file))
                    .map(Path::toFile)
                    .filter(isArchivePredicate)
                    .collect(Collectors.toList());

            int maxThreads = Math.max(files.size(), MAX_PARALLEL_THREADS);
            ExecutorService threadPool = Executors.newFixedThreadPool(maxThreads);

            List<Callable<Integer>> tasks = files
                    .stream()
                    .map(Main::createTask)
                    .collect(Collectors.toList());

            List<Future<Integer>> results = threadPool.invokeAll(tasks);
            threadPool.shutdown();

            int grandTotal = results
                    .stream()
                    .mapToInt(Main::getTaskResult)
                    .sum();

            log.info("Grand total data lines have been processed in files: {}", grandTotal);
            log.info("Saved rows in the database:");
            dbManager.showQueryResults("select count(*) from igra_data");
        } catch (SQLException e) {
            log.error("Unable to connect to database", e);
        } catch (IOException e) {
            log.error("Unable to read input path", e);
        } catch (InterruptedException e) {
            log.error("Unable to execute tasks", e);
        }
    }

    private static Callable<Integer> createTask(File file) {
        return () -> {
            DbManager dbManager = new DbManager(config);

            try (Connection ignored = dbManager.createDatabaseConnection()) {
                IgraFileReader fileReader = new IgraFileReader(dbManager);
                return processFile(fileReader, file);
            }
        };
    }

    private static Integer getTaskResult(Future<Integer> future) {
        Integer result = null;
        try {
            result = future.get();
        } catch (InterruptedException | ExecutionException ignored1) {
        }

        return result;
    }

    private static int processFile(IgraFileReader fileReader, File file) {
        int totalLines;

        long startTime = System.currentTimeMillis();
        log.info("Processing input file: {}", file.getPath());
        totalLines = fileReader.processIgraData(file);
        Duration duration = Duration.ofMillis(System.currentTimeMillis() - startTime);
        log.info("File has been processed: {} seconds", duration.getSeconds());

        return totalLines;
    }

    private static Properties getProperties(String fileName) {
        Properties prop = new Properties();

        try (InputStream input = new FileInputStream(fileName)) {
            prop.load(input);
        } catch (IOException e) {
            log.error("Unable to load properties file", e);
        }

        return prop;
    }
}
