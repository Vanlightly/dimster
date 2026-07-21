package dev.dimster.coordinator;

/**
 * A running benchmark engine that can accept coordinator-side live actions.
 *
 * <p>The coordinator owns only the HTTP lifecycle and delegates mode-specific
 * validation to the active engine. This interface keeps the coordinator from
 * depending on one concrete engine while still allowing modes to expose a
 * deliberately narrow modification surface.
 */
@FunctionalInterface
public interface ModificationTarget {

    /** Validate and apply one live modification request. */
    ModifyResult applyModification(ModifyRequest request);
}
