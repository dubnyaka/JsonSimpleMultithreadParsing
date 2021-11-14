import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import entity.TrafficViolation;
import org.apache.commons.math3.util.Precision;

import java.io.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class JsonGenerator {

    public static void main(String[] args) {
        generate("src/main/input/",50,2,3);
    }


    public static void generate(String dir,int persons,int maxPerPerson,int count){
        for (int counter = 0; counter < count; counter++) {
            String fileName = String.format("traffic_violation%d.json",counter);
            List<String> violationTypes = Arrays.asList("SPEEDING", "PARKING", "AGGRESSIVE DRIVING");
            // Сделать для рандомного имени и фамилии несколько штрафов

            List<String> FirstNames = new ArrayList<>();
            List<String> LastNames = new ArrayList<>();

            String FirstNamesFille = "src/main/java/UtilsData/EnglishNames.txt";
            String LastNamesFille = "src/main/java/UtilsData/EnglishLastNames.txt";

            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(FirstNamesFille));
                FirstNames.addAll(br.lines().toList());

                br = new BufferedReader(new FileReader(LastNamesFille));
                LastNames.addAll(br.lines().toList());

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            Random rand = new Random();

            List<TrafficViolation> violationsList = new ArrayList<>();
            // How many person
            for (int i = 0; i < persons; i++) {
                String tempFirstName = FirstNames.get(rand.nextInt(FirstNames.size()));
                String tempLastName = LastNames.get(rand.nextInt(LastNames.size()));
                UUID PersonID = UUID.randomUUID();

                // How many violation for one person
                for (int j = 0; j < rand.nextInt(maxPerPerson) + 1; j++) {
                    LocalDateTime date_time = LocalDateTime.now()
                            .minusYears(rand.nextInt(10))
                            .minusMonths(rand.nextInt(20))
                            .minus(rand.nextInt(1000000), ChronoUnit.SECONDS);
                    String type = violationTypes.get(rand.nextInt(violationTypes.size()));
                    double fine_amount = Precision.round(rand.nextDouble(5000.0),2);

                    TrafficViolation violation = new TrafficViolation(date_time, tempFirstName, tempLastName, type, fine_amount);
                    violationsList.add(violation);
                }
            }
            Collections.shuffle(violationsList);

            // Serialization to json
            ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
            try {
                objectMapper.writeValue(new File(dir + fileName),violationsList);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
