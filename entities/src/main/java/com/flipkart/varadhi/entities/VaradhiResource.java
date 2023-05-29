package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.utils.JsonMapper;
import lombok.Getter;
import lombok.Setter;

@Getter
public class VaradhiResource {

    private final String name;

    @Setter
    private int version;

    protected VaradhiResource(String name, int version) {
        this.name = name;
        this.version = version;
    }

//    public static <T> String jsonSerialize(T entity) {
//        return JsonMapper.jsonSerialize(entity);
//    }
//
//    public static <T> T jsonDeserialize(String data, Class<T> clazz) {
//        return JsonMapper.jsonDeserialize(data, clazz);
//    }
}
