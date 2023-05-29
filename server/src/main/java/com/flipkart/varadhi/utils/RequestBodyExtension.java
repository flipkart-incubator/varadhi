package com.flipkart.varadhi.utils;

import io.vertx.ext.web.RequestBody;

public class RequestBodyExtension {

    /*
    Extension method for vertx RequestBody.
    builtin asPojo() method is not working because of jackson issues i.e.
    it needs default constructor and none final fields.

    Extending RequestBody to have asPojo() custom deserializer to convert requestBody to appropriate Pojo.
     */
    public static <T> T asPojo(RequestBody body, Class<T> clazz) {
        return JsonMapper.jsonDeserialize(body.asString(), clazz);
    }
}
