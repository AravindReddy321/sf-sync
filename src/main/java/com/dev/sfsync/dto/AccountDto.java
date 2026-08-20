package com.dev.sfsync.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.extern.jackson.Jacksonized;

@Builder
//@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public record AccountDto(
        @JsonProperty(value = "Id") String id,
        @JsonProperty(value = "Name") String name,
        @JsonProperty(value = "Description") String description,
        @JsonProperty(value = "IsDeleted") boolean isDeleted
) {
}
