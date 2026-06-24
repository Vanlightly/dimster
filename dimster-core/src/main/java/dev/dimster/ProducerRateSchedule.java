package dev.dimster;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Describes a time-varying producer rate for run-mode producer load.
 *
 * The schedule exists beside the historical scalar {@code producerRate} field:
 * {@code producerRate} remains the nominal/max rate used by older workloads
 * and resource estimation, while this object controls the live rate when it
 * is present on a test point.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProducerRateSchedule {

    /** The high-level schedule shape the worker evaluates at send time. */
    public enum Type {
        /**
         * Smooth cosine wave starting at {@code minRate}, reaching
         * {@code maxRate} halfway through the period, then returning to
         * {@code minRate}.
         */
        OSCILLATING,

        /** Ordered list of fixed-rate holds and optional smooth ramps. */
        REPEATING
    }

    /** Interpolation style used by ramping repeating segments. */
    public enum Interpolation {
        LINEAR,
        SMOOTH
    }

    public Type type;

    // OSCILLATING fields.
    public double minRate;
    public double maxRate;
    public double periodSeconds;

    // REPEATING fields. The segment sequence always repeats.
    public List<Segment> segments;

    /**
     * One piece of a repeating schedule. If {@code rate} is set, the segment
     * is a hold. Otherwise {@code fromRate} and {@code toRate} define a ramp.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Segment {
        public Double rate;
        public Double fromRate;
        public Double toRate;
        public double durationSeconds;
        public Interpolation interpolation = Interpolation.SMOOTH;

        public Segment copyDividedBy(double divisor) {
            Segment copy = new Segment();
            copy.rate = rate == null ? null : rate / divisor;
            copy.fromRate = fromRate == null ? null : fromRate / divisor;
            copy.toRate = toRate == null ? null : toRate / divisor;
            copy.durationSeconds = durationSeconds;
            copy.interpolation = interpolation;
            return copy;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Segment segment)) return false;
            return Double.compare(durationSeconds, segment.durationSeconds) == 0
                    && Objects.equals(rate, segment.rate)
                    && Objects.equals(fromRate, segment.fromRate)
                    && Objects.equals(toRate, segment.toRate)
                    && interpolation == segment.interpolation;
        }

        @Override
        public int hashCode() {
            return Objects.hash(rate, fromRate, toRate, durationSeconds, interpolation);
        }
    }

    /**
     * Validate user input before the worker starts using the schedule. This
     * catches YAML mistakes close to workload expansion and keeps the hot send
     * loop free of defensive shape checks.
     */
    public void validate() {
        if (type == null) {
            throw new IllegalArgumentException("producerRateSchedule.type is required");
        }

        switch (type) {
            case OSCILLATING -> validateOscillating();
            case REPEATING -> validateRepeating();
        }
    }

    /** Return the rate that should apply at the given elapsed schedule time. */
    public double rateAt(Duration elapsed) {
        validate();
        double seconds = Math.max(0.0, elapsed.toNanos() / 1_000_000_000.0);
        return switch (type) {
            case OSCILLATING -> oscillatingRate(seconds);
            case REPEATING -> repeatingRate(seconds);
        };
    }

    /**
     * Maximum rate the schedule can produce. Engines use this as the effective
     * target when they need one scalar for logs, result metadata, or sizing.
     */
    public double maxConfiguredRate() {
        validate();
        if (type == Type.OSCILLATING) {
            return maxRate;
        }
        double max = 0.0;
        for (Segment s : segments) {
            if (s.rate != null) {
                max = Math.max(max, s.rate);
            } else {
                max = Math.max(max, Math.max(s.fromRate, s.toRate));
            }
        }
        return max;
    }

    /**
     * First rate produced when the schedule starts. Run-mode ramp-up uses this
     * as its target so a scheduled workload can warm producers up gently before
     * the schedule clock begins.
     */
    public double firstRate() {
        validate();
        return switch (type) {
            case OSCILLATING -> minRate;
            case REPEATING -> segmentRateAt(segments.getFirst(), 0.0);
        };
    }

    /** Return a deep copy whose rates are divided for per-worker fan-out. */
    public ProducerRateSchedule copyDividedBy(double divisor) {
        if (divisor <= 0.0) {
            throw new IllegalArgumentException("divisor must be > 0");
        }
        ProducerRateSchedule copy = new ProducerRateSchedule();
        copy.type = type;
        copy.minRate = minRate / divisor;
        copy.maxRate = maxRate / divisor;
        copy.periodSeconds = periodSeconds;
        if (segments != null) {
            copy.segments = new ArrayList<>(segments.size());
            for (Segment segment : segments) {
                copy.segments.add(segment.copyDividedBy(divisor));
            }
        }
        return copy;
    }

    private void validateOscillating() {
        requireNonNegative("producerRateSchedule.minRate", minRate);
        requirePositive("producerRateSchedule.maxRate", maxRate);
        requirePositive("producerRateSchedule.periodSeconds", periodSeconds);
        if (maxRate < minRate) {
            throw new IllegalArgumentException("producerRateSchedule.maxRate must be >= minRate");
        }
    }

    private void validateRepeating() {
        if (segments == null || segments.isEmpty()) {
            throw new IllegalArgumentException("producerRateSchedule.segments must not be empty");
        }
        boolean hasPositiveRate = false;
        for (int i = 0; i < segments.size(); i++) {
            Segment segment = segments.get(i);
            String prefix = "producerRateSchedule.segments[" + i + "]";
            requirePositive(prefix + ".durationSeconds", segment.durationSeconds);

            boolean hold = segment.rate != null;
            boolean ramp = segment.fromRate != null || segment.toRate != null;
            if (hold == ramp) {
                throw new IllegalArgumentException(prefix + " must define either rate or fromRate/toRate");
            }
            if (hold) {
                requireNonNegative(prefix + ".rate", segment.rate);
                hasPositiveRate |= segment.rate > 0.0;
            } else {
                if (segment.fromRate == null || segment.toRate == null) {
                    throw new IllegalArgumentException(prefix + " ramp requires both fromRate and toRate");
                }
                requireNonNegative(prefix + ".fromRate", segment.fromRate);
                requireNonNegative(prefix + ".toRate", segment.toRate);
                hasPositiveRate |= segment.fromRate > 0.0 || segment.toRate > 0.0;
            }
        }
        if (!hasPositiveRate) {
            throw new IllegalArgumentException("producerRateSchedule must contain at least one positive rate");
        }
    }

    private double oscillatingRate(double elapsedSeconds) {
        double phase = (elapsedSeconds % periodSeconds) / periodSeconds;
        double wave = (1.0 - Math.cos(2.0 * Math.PI * phase)) / 2.0;
        return minRate + (maxRate - minRate) * wave;
    }

    private double repeatingRate(double elapsedSeconds) {
        double cycleSeconds = 0.0;
        for (Segment segment : segments) {
            cycleSeconds += segment.durationSeconds;
        }

        double position = elapsedSeconds % cycleSeconds;

        for (Segment segment : segments) {
            if (position <= segment.durationSeconds) {
                return segmentRateAt(segment, position / segment.durationSeconds);
            }
            position -= segment.durationSeconds;
        }
        return segmentEndRate(segments.getLast());
    }

    private static double segmentRateAt(Segment segment, double rawProgress) {
        if (segment.rate != null) {
            return segment.rate;
        }
        double progress = Math.max(0.0, Math.min(1.0, rawProgress));
        if (segment.interpolation == Interpolation.SMOOTH) {
            progress = progress * progress * (3.0 - 2.0 * progress);
        }
        return segment.fromRate + (segment.toRate - segment.fromRate) * progress;
    }

    private static double segmentEndRate(Segment segment) {
        return segment.rate != null ? segment.rate : segment.toRate;
    }

    /**
     * Return how long a zero-rate schedule position should wait before the
     * next positive-rate point. Zero is allowed as a deliberate pause, but the
     * producer loop still needs a concrete next send time.
     */
    public Duration durationUntilNextPositiveRate(Duration elapsed) {
        validate();
        if (rateAt(elapsed) > 0.0) {
            return Duration.ZERO;
        }
        if (type == Type.OSCILLATING) {
            return Duration.ofMillis(1);
        }

        double cycleSeconds = 0.0;
        for (Segment segment : segments) {
            cycleSeconds += segment.durationSeconds;
        }

        double position = Math.max(0.0, elapsed.toNanos() / 1_000_000_000.0) % cycleSeconds;
        double waitSeconds = 0.0;
        for (int checked = 0; checked < segments.size() * 2; checked++) {
            for (Segment segment : segments) {
                if (position > segment.durationSeconds) {
                    position -= segment.durationSeconds;
                    continue;
                }
                double remaining = segment.durationSeconds - position;
                if (segment.rate != null) {
                    if (segment.rate > 0.0) {
                        return secondsToDuration(waitSeconds);
                    }
                } else if (segment.toRate > 0.0 && position < segment.durationSeconds) {
                    return secondsToDuration(waitSeconds + 0.001);
                }
                waitSeconds += remaining;
                position = 0.0;
            }
        }
        return secondsToDuration(waitSeconds);
    }

    private static Duration secondsToDuration(double seconds) {
        return Duration.ofNanos(Math.max(1L, Math.round(seconds * 1_000_000_000.0)));
    }

    private static void requirePositive(String field, double value) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value <= 0.0) {
            throw new IllegalArgumentException(field + " must be > 0");
        }
    }

    private static void requireNonNegative(String field, double value) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value < 0.0) {
            throw new IllegalArgumentException(field + " must be >= 0");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProducerRateSchedule that)) return false;
        return Double.compare(minRate, that.minRate) == 0
                && Double.compare(maxRate, that.maxRate) == 0
                && Double.compare(periodSeconds, that.periodSeconds) == 0
                && type == that.type
                && Objects.equals(segments, that.segments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, minRate, maxRate, periodSeconds, segments);
    }
}
