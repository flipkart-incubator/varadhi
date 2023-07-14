package com.flipkart.varadhi.entities;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.FieldDefaults;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Size;

@FieldDefaults(level = AccessLevel.PRIVATE)
@EqualsAndHashCode(callSuper = true)
@Getter
@Value
public class OrgEntity extends VaradhiEntity {

    static final int ORG_NAME_LENGTH = 15;

    public OrgEntity(@Size(max = ORG_NAME_LENGTH) @NotEmpty String name) {
        super(name);
    }

}
