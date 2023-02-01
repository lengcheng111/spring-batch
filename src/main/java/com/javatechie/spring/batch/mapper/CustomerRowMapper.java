package com.javatechie.spring.batch.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.javatechie.spring.batch.entity.Customer;
import org.springframework.jdbc.core.RowMapper;

public class CustomerRowMapper implements RowMapper<Customer> {

    @Override
    public Customer mapRow(ResultSet rs, int rowNum) throws SQLException {
        Customer customer = new Customer();
        customer.setId(rs.getInt("customer_id"));
        customer.setFirstName(rs.getString("first_name"));
        customer.setLastName(rs.getString("last_name"));
        customer.setDob(rs.getString("dob"));
        customer.setGender(rs.getString("gender"));
        customer.setCountry(rs.getString("country"));
        customer.setEmail(rs.getString("email"));
        customer.setContactNo(rs.getString("contact"));
        return customer;
    }
}