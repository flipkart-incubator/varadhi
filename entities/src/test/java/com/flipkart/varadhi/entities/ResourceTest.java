package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ResourceTest {

    @Test
    void testOrgResourceSerializationAndDeserialization() throws Exception {
        ObjectMapper mapper = JsonMapper.getMapper();

        // Create an EntityResource object
        Org mockEntity = new Org("testEntity", 1);
        OrgDetails originalResource = new OrgDetails(mockEntity, new OrgFilters(0, Map.of()));

        String jsonData = mapper.writeValueAsString(originalResource);
        Resource deserializedResource = mapper.readValue(jsonData, Resource.class);

        assertEquals(originalResource.getName(), deserializedResource.getName());
        assertEquals(originalResource.getVersion(), deserializedResource.getVersion());
    }

    @Test
    void testEntityResourceSerializationAndDeserialization() throws Exception {
        ObjectMapper mapper = JsonMapper.getMapper();

        // Create an EntityResource object
        Project p = Project.of("proj", "desc", "team", "org");
        Resource.EntityResource<Project> originalResource = Resource.of(p, ResourceType.PROJECT);

        String jsonData = mapper.writeValueAsString(originalResource);
        System.out.println(jsonData);

        Resource.EntityResource<?> deserializedResource = (Resource.EntityResource<?>)mapper.readValue(
            jsonData,
            Resource.class
        );

        assertEquals(originalResource.getName(), deserializedResource.getName());
        assertEquals(originalResource.getVersion(), deserializedResource.getVersion());
        assertEquals(originalResource.getEntity(), deserializedResource.getEntity());
    }
}
