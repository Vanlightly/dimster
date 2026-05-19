package dev.dimster.engines.run;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.dimster.coordinator.ModifyResult;
import dev.dimster.worker.Worker;

/**
 * Executes a {@link TestPointMutator.MutationPlan} action-by-action against
 * the workers, returning the list of human-readable action descriptions
 * that succeeded (for the per-test-point modifications log) and a
 * {@link ModifyResult} describing the outcome.
 *
 * Per the user's "best-effort apply, log per-action" decision: the applier
 * issues each action in order and aborts at the first failure, returning a
 * rejection result that names which action failed and which had already
 * succeeded. The succeeded actions remain applied — there is no rollback,
 * which is consistent with the existing behavior for half-applied modify
 * requests.
 */
public class DiffApplier {

    private static final Logger log = LoggerFactory.getLogger(DiffApplier.class);

    private final Worker worker;

    public DiffApplier(Worker worker) {
        this.worker = worker;
    }

    /**
     * Apply the plan and return:
     *  - a {@link Result} carrying the human-readable action descriptions
     *    that the modifications log should record, and
     *  - a {@link ModifyResult} summarizing success/failure for the HTTP
     *    response.
     */
    public Result apply(TestPointMutator.MutationPlan plan) {
        List<String> appliedActions = new ArrayList<>(plan.actions().size());
        if (plan.actions().isEmpty()) {
            log.info("MODIFY no-op (override resulted in no diff)");
            return new Result(appliedActions,
                    ModifyResult.ok("no-op (no actions needed)"));
        }
        for (DiffAction action : plan.actions()) {
            try {
                log.info("MODIFY action: {}", action.describe());
                action.apply(worker);
                appliedActions.add(action.describe());
            } catch (Exception e) {
                String msg = "action '" + action.describe() + "' failed: " + e.getMessage();
                log.error("MODIFY {}; {}/{} actions applied",
                        msg, appliedActions.size(), plan.actions().size(), e);
                return new Result(appliedActions,
                        ModifyResult.rejected(appliedActions.size() + " of "
                                + plan.actions().size() + " actions applied; " + msg));
            }
        }
        log.info("MODIFY complete ({} actions applied)", appliedActions.size());
        return new Result(appliedActions,
                ModifyResult.ok(appliedActions.size() + " actions applied"));
    }

    /**
     * @param appliedActions the {@link DiffAction#describe()} strings of
     *     every action that the worker accepted, in order. The caller
     *     attaches these to the per-test-point modifications log.
     * @param modifyResult   the outcome shape the HTTP /modify response
     *     should carry.
     */
    public record Result(List<String> appliedActions, ModifyResult modifyResult) {}
}
