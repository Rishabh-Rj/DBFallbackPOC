package net.javaguides.ems.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EmployeeEvent {
    private Long id;
    private String firstName;
    private String lastName;
    private String email;
}
