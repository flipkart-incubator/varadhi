package com.flipkart.varadhi.web.handlers;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.json.schema.JsonSchema;
import io.vertx.json.schema.JsonSchemaOptions;
import io.vertx.json.schema.OutputUnit;
import io.vertx.json.schema.Validator;
import lombok.extern.slf4j.Slf4j;

import static io.vertx.json.schema.common.dsl.Keywords.maxLength;
import static io.vertx.json.schema.common.dsl.Keywords.minLength;
import static io.vertx.json.schema.common.dsl.Schemas.*;

@Slf4j
public class TopicSchemaValidationHandler implements ValidationHandler {

    @Override
    public void handle(RoutingContext event) {

        event.request().bodyHandler(bodyHandler -> {
            final JsonObject body = bodyHandler.toJsonObject();
            JsonObject schema = objectSchema()
                    .requiredProperty("name", stringSchema().with(minLength(5)).with(maxLength(100)))
                    .requiredProperty("project", stringSchema().with(minLength(5)).with(maxLength(100)))
                    .requiredProperty("grouped", booleanSchema())
                    .toJson();

            OutputUnit outputUnit = Validator.create(JsonSchema.of(schema), new JsonSchemaOptions()).validate(body);
            if (outputUnit.getValid()) {
                event.next();
            }
            event.fail(400);
        });
    }
}
