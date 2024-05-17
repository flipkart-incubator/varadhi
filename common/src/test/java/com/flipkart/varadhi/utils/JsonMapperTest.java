package com.flipkart.varadhi.utils;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.exceptions.VaradhiException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;


public class JsonMapperTest {
    private static ObjectMapper objectMapper;

    @BeforeAll
    public static void setUp() {
        objectMapper = JsonMapper.getMapper();

        // Register subtypes at runtime
        objectMapper.registerSubtypes(new NamedType(Car.class, "car"));
        objectMapper.registerSubtypes(new NamedType(zoobar.class, "zoobar"));
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
    public void test_test() {
        Vehicle car = new Car("Honda", "Civic", 2022);
        zoobar<Vehicle> zoobar = new zoobar<>(car);
        String json = JsonMapper.jsonSerialize(zoobar);
        assertNotNull(json);
        zoobar<Vehicle> deserialized = JsonMapper.jsonDeserialize(json, zoobar.class);
        assertNotNull(deserialized);
        assertEquals(car.getManufacturer(), deserialized.data.getManufacturer());
        assertEquals(((Car)car).year, ((Car)deserialized.data).year);
    }

    @Test
    public void testJsonDeserialize_InvalidPolymorphicData() {
        String invalidJson = "{\"manufacturer\":\"Honda\",\"@vehicleType\":\"InvalidType\"}";
        Exception exception =
                assertThrows(VaradhiException.class, () -> JsonMapper.jsonDeserialize(invalidJson, Vehicle.class));
        assertTrue(exception.getMessage().contains("Could not resolve type id 'InvalidType'"));
    }

    @Test
    public void testJsonSerializeFailure() {
        CantSerialize obj = new CantSerialize();
        Exception exception = assertThrows(VaradhiException.class, () -> JsonMapper.jsonSerialize(obj));
        Assertions.assertEquals(InvalidDefinitionException.class, exception.getCause().getClass());
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@vehicleType")
    @EqualsAndHashCode
    private abstract static class Vehicle {
        private String manufacturer;

        public String getManufacturer() {
            return manufacturer;
        }

        public void setManufacturer(String manufacturer) {
            this.manufacturer = manufacturer;
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Getter
    private static class Car extends Vehicle {
        private final String model;
        private final int year;

        public Car(String manufacturer, String model, int year) {
            setManufacturer(manufacturer);
            this.model = model;
            this.year = year;
        }
    }

    private static class CantSerialize {
        private int foobar;

        private void CantSerialize() {

        }
    }

    public class foobar {
        String id;
        long time;

        public foobar() {
            id = UUID.randomUUID().toString();
            time = System.currentTimeMillis();
        }
        public foobar(String id, long time) {
            this.id = id;
            this.time = time;
        }
    }

    public class zoobar<T> extends foobar {
        T data;
        public zoobar() {
            super();
        }
        public zoobar(T t) {
            super();
            data = t;
        }
        public zoobar(String id, long time, T t) {
            super(id, time);
            data = t;
        }
    }
 }
