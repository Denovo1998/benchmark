/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark;

/** One fixed-duration sample emitted by the framework. */
public class PeriodSample {
    /** Wall-clock timestamp used to align R1 fault events with one-second samples. */
    public String timestamp;

    public long elapsedSeconds;
    public long messagesSent;
    public long messagesReceived;
    public long messageSendErrors;
    public long bytesSent;
    public long bytesReceived;
    public long inFlightSends;
    public long backlog;
    public double publishRate;
    public double publishThroughputMiB;
    public double consumeRate;
    public double consumeThroughputMiB;
    public double publishErrorRate;
    public double publishLatencyP99Ms;
    public double endToEndLatencyP99Ms;
}
