package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.common.utils.JsonMapper;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ResourceTest {

    @Test
    void testEntityResourceSerializationAndDeserialization() throws Exception {
        // Create an EntityResource object
        Org mockEntity = new Org("testEntity", 1);
        MetaStoreEntityType mockMetaStoreEntityType = MetaStoreEntityType.ORG;
        OrgDetails originalResource = new OrgDetails(mockEntity, new OrgFilters(0, Map.of()));

        ObjectMapper objectMapper = JsonMapper.getMapper();

        byte[] serializedData = objectMapper.writeValueAsBytes(originalResource);
        String jsonData = objectMapper.writeValueAsString(originalResource);
        Resource deserializedResource = objectMapper.readValue(jsonData, Resource.class);

        assertEquals(originalResource.getName(), deserializedResource.getName());
        assertEquals(originalResource.getVersion(), deserializedResource.getVersion());
    }

}
