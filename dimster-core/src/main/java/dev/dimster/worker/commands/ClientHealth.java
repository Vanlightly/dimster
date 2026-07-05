package dev.dimster.worker.commands;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Compact health snapshot returned by worker /health and consumed by the
 * coordinator. The class exists to keep high-cardinality workers from
 * serializing thousands of healthy ClientStatus entries while preserving the
 * existing status model used by readiness checks: PENDING, RUNNING,
 * RECREATING, and FAILED.
 */
public record ClientHealth(
        ClientCounts producers,
        ClientCounts consumers,
        List<ClientStatus> failedClients,
        int failedClientLimit,
        int omittedFailedClients) {

    public static final int DEFAULT_FAILED_CLIENT_LIMIT = 100;

    public ClientHealth {
        producers = producers == null ? ClientCounts.empty() : producers;
        consumers = consumers == null ? ClientCounts.empty() : consumers;
        failedClients = failedClients == null ? List.of() : List.copyOf(failedClients);
    }

    public static ClientHealth empty() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder(DEFAULT_FAILED_CLIENT_LIMIT);
    }

    public static Builder builder(int failedClientLimit) {
        return new Builder(failedClientLimit);
    }

    /**
     * Aggregates per-worker health into the same compact shape the engines use.
     * The failed-client detail cap is applied again at the aggregate level so a
     * large worker count cannot recreate the oversized response problem in the
     * coordinator.
     */
    public static ClientHealth aggregate(List<ClientHealth> workerHealth) {
        Builder aggregate = builder();
        for (ClientHealth health : workerHealth) {
            if (health == null) {
                continue;
            }
            aggregate.producers.add(health.producers());
            aggregate.consumers.add(health.consumers());
            aggregate.addFailedClients(health.failedClients());
            aggregate.omittedFailedClients += health.omittedFailedClients();
        }
        return aggregate.build();
    }

    @JsonIgnore
    public boolean hasFailures() {
        return producers.failed() + consumers.failed() > 0;
    }

    @JsonIgnore
    public boolean isEmpty() {
        return producers.total() + consumers.total() == 0;
    }

    @JsonIgnore
    public boolean allRunning() {
        return producers.allRunning() && consumers.allRunning();
    }

    @JsonIgnore
    public ClientCounts countsFor(String clientTypeFilter) {
        if ("PRODUCER".equals(clientTypeFilter)) {
            return producers;
        }
        if ("CONSUMER".equals(clientTypeFilter)) {
            return consumers;
        }
        return producers.plus(consumers);
    }

    @JsonIgnore
    public RuntimeException failureException() {
        String omitted = omittedFailedClients > 0
                ? " (" + omittedFailedClients + " additional failed clients omitted)"
                : "";
        if (!failedClients.isEmpty()) {
            ClientStatus cs = failedClients.get(0);
            return new RuntimeException("Client '" + cs.name() + "' (" + cs.clientType()
                    + ") failed: " + cs.error() + omitted);
        }
        return new RuntimeException("Client health reported "
                + (producers.failed() + consumers.failed())
                + " failed clients, but no failed client details were included" + omitted);
    }

    /**
     * Status counts for one broad client category. Producers are reported as
     * PRODUCER, while both CLASSIC_CONSUMER and SHARE_CONSUMER roll into the
     * consumer counts because engine readiness only needs producer-vs-consumer
     * gating.
     */
    public record ClientCounts(int total, int running, int pending, int recreating, int failed) {

        public static ClientCounts empty() {
            return new ClientCounts(0, 0, 0, 0, 0);
        }

        public ClientCounts plus(ClientCounts other) {
            return new ClientCounts(
                    total + other.total,
                    running + other.running,
                    pending + other.pending,
                    recreating + other.recreating,
                    failed + other.failed);
        }

        public boolean allRunning() {
            return running == total;
        }
    }

    public static final class Builder {
        private final int failedClientLimit;
        private final CountsBuilder producers = new CountsBuilder();
        private final CountsBuilder consumers = new CountsBuilder();
        private final List<ClientStatus> failedClients = new ArrayList<>();
        private int omittedFailedClients;

        private Builder(int failedClientLimit) {
            this.failedClientLimit = failedClientLimit;
        }

        public Builder addProducer(ClientStatus status) {
            producers.add(status);
            addFailedClient(status);
            return this;
        }

        public Builder addConsumer(ClientStatus status) {
            consumers.add(status);
            addFailedClient(status);
            return this;
        }

        public ClientHealth build() {
            return new ClientHealth(
                    producers.build(),
                    consumers.build(),
                    failedClients,
                    failedClientLimit,
                    omittedFailedClients);
        }

        private void addFailedClients(List<ClientStatus> statuses) {
            for (ClientStatus status : statuses) {
                addFailedClient(status);
            }
        }

        private void addFailedClient(ClientStatus status) {
            if (status == null || !ClientStatus.FAILED.equals(status.status())) {
                return;
            }
            if (failedClients.size() < failedClientLimit) {
                failedClients.add(status);
            } else {
                omittedFailedClients++;
            }
        }
    }

    private static final class CountsBuilder {
        private int total;
        private int running;
        private int pending;
        private int recreating;
        private int failed;

        void add(ClientStatus status) {
            total++;
            switch (status.status()) {
                case ClientStatus.RUNNING -> running++;
                case ClientStatus.PENDING -> pending++;
                case ClientStatus.RECREATING -> recreating++;
                case ClientStatus.FAILED -> failed++;
                default -> throw new IllegalArgumentException("Unknown client status: " + status.status());
            }
        }

        void add(ClientCounts counts) {
            total += counts.total();
            running += counts.running();
            pending += counts.pending();
            recreating += counts.recreating();
            failed += counts.failed();
        }

        ClientCounts build() {
            return new ClientCounts(total, running, pending, recreating, failed);
        }
    }
}
