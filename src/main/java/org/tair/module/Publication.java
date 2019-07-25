package org.tair.module;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
// CLASS /DATA STRUCTURE DECLARATION
public class Publication { //THIRD CLASS CREATED

    //Attributes Declaration
    private String date;
    private AuthorList authorList;
    private String title;
    private String name;
}


@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
class AuthorList<T> {
    private List<Person> person = new ArrayList<>();

    @JsonProperty("person")
    private void buildSequenceInfo(T person) throws JsonProcessingException {
        if (person instanceof List) {
            this.person = (List<Person>) person;
        } else {
            final ObjectMapper mapper = new ObjectMapper(); // jackson's objectmapper
            final Person personObj = mapper.convertValue(person, Person.class);
            this.person.add(personObj);
        }
    }
}

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
class Person {
    private String name;
}
