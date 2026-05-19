package dev.dimster.coordinator;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ResumeRequest(
    List<String> workers
) {}
