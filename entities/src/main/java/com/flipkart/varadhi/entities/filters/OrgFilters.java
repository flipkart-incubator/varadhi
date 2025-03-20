package com.flipkart.varadhi.entities.filters;

import com.flipkart.varadhi.entities.MetaStoreEntity;
import lombok.Getter;

import java.util.List;

@Getter
public class OrgFilters extends MetaStoreEntity {
    String name;
    List<NamedConditions> namedConditions;

    protected OrgFilters(String name, int version) {
        super(name, version);
    }
}
