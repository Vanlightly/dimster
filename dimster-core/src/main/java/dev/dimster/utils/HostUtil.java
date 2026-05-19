package dev.dimster.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

/**
 * Single source of truth for the local host identifier used by both Prometheus
 * metric labels and producer transactional.id construction. Keeping these in
 * sync matters because the transactional.id generated in
 * {@code LocalWorker.createProducers} embeds the same hostname that the
 * /metrics endpoint reports under the {@code host="..."} label, which lets
 * dashboards correlate per-host transaction metrics with the producers that
 * emitted them.
 *
 * <p>The fallback returns a random number string (preserving prior
 * {@code PrometheusEndpoint.getHost} behavior). For benchmarking that's
 * acceptable: a worker without a resolvable hostname is rare, and using a
 * random fallback at least keeps multiple unresolvable workers from colliding
 * on the same transactional.id within a single run.
 */
public final class HostUtil {

    private HostUtil() {}

    public static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return String.valueOf(new Random().nextInt(10000));
        }
    }
}
