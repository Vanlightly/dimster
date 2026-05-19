package dev.dimster.coordinator;

/**
 * Outcome of a {@link ModifyRequest} applied to a running engine.
 *
 * The two-state shape (accepted + message) keeps the coordinator's `/modify`
 * handler trivial: 200 with the message on accepted, 409 with the reason
 * otherwise. We deliberately don't echo back the applied numeric values —
 * the message string is human-readable and shows up verbatim in the CLI
 * response and the coordinator log, which is enough for the single human
 * operator who initiated the modify.
 */
public record ModifyResult(boolean accepted, String message) {

    public static ModifyResult ok(String message) {
        return new ModifyResult(true, message);
    }

    public static ModifyResult rejected(String reason) {
        return new ModifyResult(false, reason);
    }
}
