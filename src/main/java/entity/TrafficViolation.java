package entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TrafficViolation {
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-M-d HH:mm:ss")
    private LocalDateTime date_time;
    private String first_name;
    private String last_name;
    private String type;
    double fine_amount;
}
