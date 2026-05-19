/*
 * Derived from io.openmessaging.benchmark.utils.distributor.NoKeyDistributor
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
 * Returns a null key and lets Kafka's default sticky partitioner handle
 * placement. Used as the baseline when no key-based routing is desired.
 */
public final class NoKeyDistributor implements MessageDistributor {

    @Override
    public Target next() {
        return Target.NULL_KEY;
    }
}
