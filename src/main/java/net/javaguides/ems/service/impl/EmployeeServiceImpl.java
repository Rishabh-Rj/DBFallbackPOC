package net.javaguides.ems.service.impl;

import lombok.AllArgsConstructor;
import net.javaguides.ems.dto.EmployeeDto;
import net.javaguides.ems.entity.Employee;
import net.javaguides.ems.event.EmployeeEvent;
import net.javaguides.ems.exception.ResourceNotFoundException;
import net.javaguides.ems.mapper.EmployeeMapper;
import net.javaguides.ems.repository.EmployeeRepository;
import net.javaguides.ems.service.EmployeeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
public class EmployeeServiceImpl implements EmployeeService {

    private EmployeeRepository employeeRepository;

    @Autowired
    private KafkaTemplate<String, net.javaguides.ems.event.EmployeeEvent> kafkaTemplate;

    private static final Logger logger = LoggerFactory.getLogger(EmployeeServiceImpl.class);
    private static final String DB_CIRCUIT_BREAKER = "dbService";

    @Override
    @CircuitBreaker(name = DB_CIRCUIT_BREAKER, fallbackMethod = "fallbackSaveEmployee")
    public EmployeeDto createEmployee(EmployeeDto employeeDto) {
        Employee employee = EmployeeMapper.mapToEmployee(employeeDto);
        try {
            Employee savedEmployee = employeeRepository.save(employee);
            EmployeeDto result = EmployeeMapper.mapToEmployeeDto(savedEmployee);
            net.javaguides.ems.event.EmployeeEvent event = new net.javaguides.ems.event.EmployeeEvent(result.getId(), result.getFirstName(), result.getLastName(), result.getEmail());
            logger.info("DB save successful. Employee id={}", result.getId());
            return result;
        } catch (Exception e) {
            if (isConstraintViolation(e)) {
                logger.warn("Duplicate or constraint violation. Not retrying. Employee: {}", employeeDto);
                throw e; // Let controller/upper layer return proper response like 409
            }
            logger.warn("DB save failed. Fallback triggered for Employee: {} | Reason: {}", employeeDto, e.getMessage());
            throw e; // Let circuit breaker trigger fallback
        }
    }
//        catch (Exception e) {
//            logger.warn("DB save failed. Falling back to Kafka. Employee: {}", employeeDto, e);
//            throw e;
//        }
//    }

    private boolean isConstraintViolation(Throwable ex) {
        while (ex != null) {
            if (ex instanceof org.hibernate.exception.ConstraintViolationException ||
                    ex instanceof java.sql.SQLIntegrityConstraintViolationException) {
                return true;
            }
            ex = ex.getCause();
        }
        return false;
    }

    public EmployeeDto fallbackSaveEmployee(EmployeeDto employeeDto, Throwable t) {
//        net.javaguides.ems.event.EmployeeEvent event = new net.javaguides.ems.event.EmployeeEvent(null, employeeDto.getFirstName(), employeeDto.getLastName(), employeeDto.getEmail());
        EmployeeEvent event = new EmployeeEvent(null, employeeDto.getFirstName(), employeeDto.getLastName(), employeeDto.getEmail());
        try {
            kafkaTemplate.send("employee-events", event);
            logger.warn("Fallback triggered. Sent event to Kafka for Employee: {} | Reason: {}", employeeDto, t.getMessage());
        } catch (Exception ex) {
            logger.error("Fallback Kafka send failed for Employee: {} | Error: {}", employeeDto, ex.getMessage(), ex);
        }

//        return employeeDto;
//        kafkaTemplate.send("employee-events", event);
//        logger.warn("Fallback: Employee event sent to Kafka. Employee: {}", employeeDto);
   return employeeDto;
    }

    // Kafka consumer for recovery
    @KafkaListener(topics = "employee-events", groupId = "employee-consumer-group",containerFactory = "kafkaListenerContainerFactory")
    public void consumeEmployeeEvent(net.javaguides.ems.event.EmployeeEvent event, Acknowledgment ack) {
        try {
            logger.info("Attempting to save event from Kafka: {}", event);
            Employee employee = new Employee();
            employee.setFirstName(event.getFirstName());

            employee.setLastName(event.getLastName());
            employee.setEmail(event.getEmail());
            employeeRepository.save(employee);
//            ack.acknowledge();
            logger.info("Kafka event saved to DB and offset acknowledged. Employee: {}", event);
        } catch (Exception ex) {
            if (isCausedByConnectionRefused(ex)) {
                logger.warn("DB not reachable. Will retry Kafka event later. Employee: {}", event);
            } if (isConstraintViolation(ex)) {
                logger.warn("Duplicate entry. Skipping and acknowledging offset. Employee: {}", event);
                ack.acknowledge(); // ✅ prevent Kafka retry
                return;
            }else {
                logger.warn("Unexpected error during DB save. Retrying... Employee: {} | Reason: {}", event, ex.getMessage());
            }


            throw new RuntimeException("DB down — will retry silently", ex);
//                    SilentRetryException("DB down — will retry silently", ex);
        }


    }



//    public class SilentRetryException extends RuntimeException {
//        public SilentRetryException(String message, Throwable cause) {
//            super(message, cause);
//        }
//    }

    private boolean isCausedByConnectionRefused(Throwable ex) {
        while (ex != null) {
            if (ex instanceof java.net.ConnectException ||
                    (ex.getMessage() != null && ex.getMessage().toLowerCase().contains("communications link failure")) ||
                    (ex.getMessage() != null && ex.getMessage().toLowerCase().contains("connection is not available"))) {
                return true;
            }
            ex = ex.getCause();
        }
        return false;
    }

    @Override
    public EmployeeDto getEmployeeById(Long employeeId) {
        Employee employee = employeeRepository.findById(employeeId)
                .orElseThrow(() ->
                        new ResourceNotFoundException("Employee is not exists with given id : " + employeeId));

        return EmployeeMapper.mapToEmployeeDto(employee);
    }

    @Override
    public List<EmployeeDto> getAllEmployees() {
        List<Employee> employees = employeeRepository.findAll();
        return employees.stream().map((employee) -> EmployeeMapper.mapToEmployeeDto(employee))
                .collect(Collectors.toList());
    }

    @Override
    public EmployeeDto updateEmployee(Long employeeId, EmployeeDto updatedEmployee) {

        Employee employee = employeeRepository.findById(employeeId).orElseThrow(
                () -> new ResourceNotFoundException("Employee is not exists with given id: " + employeeId)
        );

        employee.setFirstName(updatedEmployee.getFirstName());
        employee.setLastName(updatedEmployee.getLastName());
        employee.setEmail(updatedEmployee.getEmail());

        Employee updatedEmployeeObj = employeeRepository.save(employee);

        return EmployeeMapper.mapToEmployeeDto(updatedEmployeeObj);
    }

    @Override
    public void deleteEmployee(Long employeeId) {

        Employee employee = employeeRepository.findById(employeeId).orElseThrow(
                () -> new ResourceNotFoundException("Employee is not exists with given id: " + employeeId)
        );

        employeeRepository.deleteById(employeeId);
    }
}
