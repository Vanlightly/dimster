package dev.dimster.worker.commands;

public record ClientStatus(String name, String clientType, String status, String error) {

    public static ClientStatus pending(String name, String clientType) {
        return new ClientStatus(name, clientType, "PENDING", null);
    }

    public static ClientStatus running(String name, String clientType) {
        return new ClientStatus(name, clientType, "RUNNING", null);
    }

    public static ClientStatus recreating(String name, String clientType, String error) {
        return new ClientStatus(name, clientType, "RECREATING", error);
    }

    public static ClientStatus failed(String name, String clientType, String error) {
        return new ClientStatus(name, clientType, "FAILED", error);
    }
}
