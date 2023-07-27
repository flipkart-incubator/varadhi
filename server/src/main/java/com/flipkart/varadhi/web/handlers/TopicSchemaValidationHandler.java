package com.flipkart.varadhi.web.handlers;

import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.json.schema.*;
import lombok.extern.slf4j.Slf4j;

import static io.vertx.json.schema.common.dsl.Keywords.maxLength;
import static io.vertx.json.schema.common.dsl.Keywords.minLength;
import static io.vertx.json.schema.common.dsl.Schemas.*;

@Slf4j
public class TopicSchemaValidationHandler implements ValidationHandler {

    @Override
    public void handle(RoutingContext event) {

        HttpServerResponse response = event.response();

        JsonObject schema = objectSchema()
                .requiredProperty("name", stringSchema().with(minLength(5)).with(maxLength(100)))
                .requiredProperty("project", stringSchema().with(minLength(5)).with(maxLength(100)))
                .requiredProperty("grouped", booleanSchema())
                .toJson();

        OutputUnit outputUnit =
                Validator.create(
                                JsonSchema.of(schema),
                                new JsonSchemaOptions().setDraft(Draft.DRAFT7).setBaseUri("http://127.0.0.1")
                                        .setOutputFormat(OutputFormat.Basic)
                        )
                        .validate(event.body().asJsonObject());
        if (outputUnit.getValid()) {
            event.next();
        }
        response.setStatusCode(400);
        JsonObject jsonObject =
                new JsonObject().put("error", "TopicSchemaValidation Failure");
        response.end(jsonObject.encodePrettily());
    }
}
