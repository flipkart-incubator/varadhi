package com.flipkart.varadhi.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;



class Vehicle {
    String name;
    public Vehicle(String name) {
        this.name = name;
    }
}

class Car extends Vehicle{
    public Car(String make, String name) {
        super(name);
        this.make = make;
    }
    String make;
}

class Bus extends  Vehicle{
    int capacity;
    public Bus(int capacity, String name){
        super(name);
        this.capacity = capacity;
    }
}


class Fleet {
    List<Vehicle> fleet = new ArrayList<>();
    String owner;
}

public class JsonMapperTest {

    @Test
    public void TestPolymorphicSerialization() {
        Fleet myfleet = new Fleet();
        myfleet.owner = "self";
        myfleet.fleet.add(new Car("honda","classic"));
        myfleet.fleet.add(new Bus(100,"Mahindra"));
        String data = JsonMapper.jsonSerialize(myfleet);
        Fleet deserialized = JsonMapper.jsonDeserialize(data, Fleet.class);
        Assertions.assertEquals(myfleet.fleet.get(0).getClass(), deserialized.fleet.get(0).getClass());
        Assertions.assertEquals(myfleet.fleet.get(1).getClass(), deserialized.fleet.get(1).getClass());
    }

}
