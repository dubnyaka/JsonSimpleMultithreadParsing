import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import entity.Offender;
import entity.TrafficViolation;
import org.apache.commons.math3.util.Precision;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class Parser {
    public static Map<String, Offender> offenders = new ConcurrentHashMap<>();
    public static Map<String, Double> totalFine = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        multithreadedParse("src/main/input", "src/main/output");
        long endTime = System.currentTimeMillis();
        System.out.println("Total execution time: " + (endTime - startTime) + "ms");
    }

    public static void multithreadedParse(String dir, String outputDir) {
        // Thread pool using the available number of threads
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        // Getting file paths from dir
        try (Stream<Path> paths = Files.walk(Paths.get(dir))) {
            List<Path> pathsList = new ArrayList<>(paths.filter(Files::isRegularFile).toList());
            // Starting a thread to process each file
            for (Path path : pathsList) {
                executorService.submit(new ParseRunnable(path));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Wait for all threads to do their work
        executorService.shutdown();
        try {
            executorService.awaitTermination(60, TimeUnit.SECONDS);
            // Calculation of the average fine amount for each offender
            offenders.forEach((key, value) -> value
                    .setAverage_fine(Precision.round(value.getTotal_fine() / value.getNumberOfViolations(), 2)));
            // Sorting total fine
            LinkedHashMap<String, Double> sortedAmountMap = new LinkedHashMap<>();
            totalFine.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .forEach(stringDoubleEntry -> sortedAmountMap.put(stringDoubleEntry.getKey(), stringDoubleEntry.getValue()));
            totalFine = new LinkedHashMap<>(sortedAmountMap);
            // Create output files
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writeValue(new File(outputDir + "/totalFine.json"), totalFine);
            objectMapper.writeValue(new File(outputDir + "/offenders.json"), offenders.values());

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void parseFile(Path filePath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        List<TrafficViolation> trafficViolationList = objectMapper.readValue(new File(filePath.toString()), new TypeReference<>() {
        });
        for (TrafficViolation violation : trafficViolationList) {
            String firstLastNameKey = violation.getFirst_name() + violation.getLast_name();
            // offender from violation
            Offender offenderFromViolation =
                    new Offender(violation.getFirst_name(), violation.getLast_name(), 1, violation.getFine_amount(), 0);

            offenders.computeIfPresent(firstLastNameKey, (key, value) -> {
                value.setNumberOfViolations(value.getNumberOfViolations() + 1);
                value.setTotal_fine(Precision.round(value.getTotal_fine() + violation.getFine_amount(), 2));
                return value;
            });
            offenders.putIfAbsent(firstLastNameKey, offenderFromViolation);

            // totalFine
            totalFine.computeIfPresent(violation.getType(), (key, value) -> Precision.round(value + violation.getFine_amount(), 2));
            totalFine.putIfAbsent(violation.getType(), violation.getFine_amount());
        }
    }

    public record ParseRunnable(Path path) implements Runnable {
        @Override
        public void run() {
            try {
                parseFile(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
