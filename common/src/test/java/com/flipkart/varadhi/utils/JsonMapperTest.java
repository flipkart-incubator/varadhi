package com.flipkart.varadhi.utils;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.exceptions.VaradhiException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class JsonMapperTest {
    private static ObjectMapper objectMapper;

    @BeforeAll
    public static void setUp() {
        objectMapper = JsonMapper.getMapper();

        // Register subtypes at runtime
        objectMapper.registerSubtypes(new NamedType(Car.class, "car"));
    }

    @Test
    public void testJsonSerializeAndDeserialize_PolymorphicObject() {
        Vehicle car = new Car("Honda", "Civic", 2022);
        String json = JsonMapper.jsonSerialize(car);
        assertNotNull(json);
        Vehicle deserialized = JsonMapper.jsonDeserialize(json, Vehicle.class);
        assertNotNull(deserialized);
        assertEquals(car.getManufacturer(), deserialized.getManufacturer());
    }

    @Test
    public void testJsonDeserialize_InvalidPolymorphicData() {
        String invalidJson = "{\"manufacturer\":\"Honda\",\"@vehicleType\":\"InvalidType\"}";
        Exception exception =
                assertThrows(VaradhiException.class, () -> JsonMapper.jsonDeserialize(invalidJson, Vehicle.class));
        assertTrue(exception.getMessage().contains("Could not resolve type id 'InvalidType'"));
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@vehicleType")
    private abstract static class Vehicle {
        private String manufacturer;

        public String getManufacturer() {
            return manufacturer;
        }

        public void setManufacturer(String manufacturer) {
            this.manufacturer = manufacturer;
        }
    }

    private static class Car extends Vehicle {
        public Car(String manufacturer, String model, int year) {
            setManufacturer(manufacturer);
        }
    }
}
