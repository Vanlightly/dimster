/*
 * Derived from io.openmessaging.benchmark.utils.distributor.KeyRoundRobin
 * in OpenMessaging Benchmark (https://github.com/openmessaging/benchmark)
 * and modified for Dimster.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.dimster.utils.distributor;

/**
 * Cycles through the shared {@link MessageDistributor#RANDOM_KEYS} pool in
 * order. Multiple producer threads share one instance and race on
 * {@code currentIndex} — the race on the field itself is benign (threads
 * may see slightly out-of-order or repeated keys near the wrap), so we
 * don't synchronize.
 *
 * <p>Correctness invariant: the array index used in {@link #next()} is the
 * post-increment value captured into a local variable, never a re-read of
 * {@code currentIndex}. Re-reading the field would expose this race:
 * thread A increments to 9999 and falls through the if-check, thread B
 * then increments the field to 10000, and A's array access reads
 * {@code currentIndex == 10000} — an {@code ArrayIndexOutOfBoundsException}.
 * Using the local {@code idx} guarantees the index is in
 * {@code [0, UNIQUE_COUNT)} regardless of how the field gets stomped on
 * by other threads.
 */
public final class KeyRoundRobin implements MessageDistributor {

    private int currentIndex = 0;

    @Override
    public Target next() {
        int idx = ++currentIndex;
        if (idx >= UNIQUE_COUNT) {
            idx = 0;
            currentIndex = 0;
        }
        return Target.ofKey(RANDOM_KEYS[idx]);
    }
}
