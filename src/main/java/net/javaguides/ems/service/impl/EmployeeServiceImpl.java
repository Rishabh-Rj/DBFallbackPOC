package net.javaguides.ems.service.impl;

import lombok.AllArgsConstructor;
import net.javaguides.ems.dto.EmployeeDto;
import net.javaguides.ems.entity.Employee;
import net.javaguides.ems.exception.ResourceNotFoundException;
import net.javaguides.ems.mapper.EmployeeMapper;
import net.javaguides.ems.repository.EmployeeRepository;
import net.javaguides.ems.service.EmployeeService;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
@AllArgsConstructor
public class EmployeeServiceImpl implements EmployeeService {

    private final EmployeeRepository employeeRepository;
    private final DataSource h2DataSource;
    private static final Logger logger = LoggerFactory.getLogger(EmployeeServiceImpl.class);

    @Override
    public EmployeeDto createEmployee(EmployeeDto employeeDto) {
        Employee employee = EmployeeMapper.mapToEmployee(employeeDto);
        try {
            net.javaguides.ems.config.DBContextHolder.setCurrentDb("mysql");
            Employee savedEmployee = employeeRepository.save(employee);
            return EmployeeMapper.mapToEmployeeDto(savedEmployee);
        } catch (Exception ex) {
            logger.warn("MySQL(primary db ) operation failed with exception "+ ex.getMessage());
            logger.warn("Switching to H2( fallback db)");
            net.javaguides.ems.config.DBContextHolder.setCurrentDb("h2");
            ensureH2TableExists();
            Employee savedEmployee = employeeRepository.save(employee);
            return EmployeeMapper.mapToEmployeeDto(savedEmployee);
        } finally {
            net.javaguides.ems.config.DBContextHolder.clear();
        }
    }

    private void ensureH2TableExists() {
        String ddl = "CREATE TABLE IF NOT EXISTS employees (id BIGINT AUTO_INCREMENT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), email_id VARCHAR(255));";
        try (Connection conn = h2DataSource.getConnection(); Statement stmt = conn.createStatement()) {
            logger.info("Ensuring H2 employees table exists (executing DDL if needed)");
            stmt.execute(ddl);
        } catch (SQLException e) {
            logger.error("Failed to create employees table in H2: {}", e.getMessage());
        }
    }

    @Override
    public EmployeeDto getEmployeeById(Long employeeId) {
        try {
            net.javaguides.ems.config.DBContextHolder.setCurrentDb("mysql");
            Employee employee = employeeRepository.findById(employeeId)
                    .orElseThrow(() ->
                            new ResourceNotFoundException("Employee is not exists with given id : " + employeeId));
            return EmployeeMapper.mapToEmployeeDto(employee);
        } catch (Exception ex) {
            logger.warn("MySQL(primary db) read failed: " + ex.getMessage());
            logger.warn("Switching to H2 (fallback db)");
            net.javaguides.ems.config.DBContextHolder.setCurrentDb("h2");
            ensureH2TableExists();
            Employee employee = employeeRepository.findById(employeeId)
                    .orElseThrow(() ->
                            new ResourceNotFoundException("Employee is not exists with given id : " + employeeId));
            return EmployeeMapper.mapToEmployeeDto(employee);
        } finally {
            net.javaguides.ems.config.DBContextHolder.clear();
        }
    }

    @Override
    public List<EmployeeDto> getAllEmployees() {
        try {
            net.javaguides.ems.config.DBContextHolder.setCurrentDb("mysql");
            List<Employee> employees = employeeRepository.findAll();
            return employees.stream().map(EmployeeMapper::mapToEmployeeDto)
                    .collect(Collectors.toList());
        } catch (Exception ex) {
            logger.warn("MySQL(primary db) readAll failed: " + ex.getMessage());
            logger.warn("Switching to H2 (fallback db)");
            net.javaguides.ems.config.DBContextHolder.setCurrentDb("h2");
            ensureH2TableExists();
            List<Employee> employees = employeeRepository.findAll();
            return employees.stream().map(EmployeeMapper::mapToEmployeeDto)
                    .collect(Collectors.toList());
        } finally {
            net.javaguides.ems.config.DBContextHolder.clear();
        }
    }

    @Override
    public EmployeeDto updateEmployee(Long employeeId, EmployeeDto updatedEmployee) {
        try {
            net.javaguides.ems.config.DBContextHolder.setCurrentDb("mysql");
            Employee employee = employeeRepository.findById(employeeId).orElseThrow(
                    () -> new ResourceNotFoundException("Employee is not exists with given id: " + employeeId)
            );
            employee.setFirstName(updatedEmployee.getFirstName());
            employee.setLastName(updatedEmployee.getLastName());
            employee.setEmail(updatedEmployee.getEmail());
            Employee updatedEmployeeObj = employeeRepository.save(employee);
            return EmployeeMapper.mapToEmployeeDto(updatedEmployeeObj);
        } catch (Exception ex) {
            logger.warn("MySQL(primary db) update failed: " + ex.getMessage());
            logger.warn("Switching to H2 (fallback db)");
            net.javaguides.ems.config.DBContextHolder.setCurrentDb("h2");
            ensureH2TableExists();
            Employee employee = employeeRepository.findById(employeeId).orElseThrow(
                    () -> new ResourceNotFoundException("Employee is not exists with given id: " + employeeId)
            );
            employee.setFirstName(updatedEmployee.getFirstName());
            employee.setLastName(updatedEmployee.getLastName());
            employee.setEmail(updatedEmployee.getEmail());
            Employee updatedEmployeeObj = employeeRepository.save(employee);
            return EmployeeMapper.mapToEmployeeDto(updatedEmployeeObj);
        } finally {
            net.javaguides.ems.config.DBContextHolder.clear();
        }
    }

    @Override
    public void deleteEmployee(Long employeeId) {
        try {
            net.javaguides.ems.config.DBContextHolder.setCurrentDb("mysql");
            Employee employee = employeeRepository.findById(employeeId).orElseThrow(
                    () -> new ResourceNotFoundException("Employee is not exists with given id: " + employeeId)
            );
            employeeRepository.deleteById(employeeId);
        } catch (Exception ex) {
            logger.warn("MySQL(primary db) delete failed: " + ex.getMessage());
            logger.warn("Switching to H2 (fallback db)");
            net.javaguides.ems.config.DBContextHolder.setCurrentDb("h2");
            ensureH2TableExists();
            Employee employee = employeeRepository.findById(employeeId).orElseThrow(
                    () -> new ResourceNotFoundException("Employee is not exists with given id: " + employeeId)
            );
            employeeRepository.deleteById(employeeId);
        } finally {
            net.javaguides.ems.config.DBContextHolder.clear();
        }
    }
}
