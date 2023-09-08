package com.flipkart.varadhi.entities;


import com.flipkart.varadhi.ValidateVaradhiResource;
import jakarta.validation.constraints.Size;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@EqualsAndHashCode(callSuper = true)
@ValidateVaradhiResource(message = "Invalid Project name. Check naming constraints.")
public class Project extends VaradhiResource {
    String org;
    @Setter
    String team;

    @Setter
    @Size(max = 100)
    String description;

    public Project(
            String name,
            int version,
            String description,
            String team,
            String org
    ) {
        super(name, version);
        this.description = description;
        this.team = team;
        this.org = org;
    }
}
