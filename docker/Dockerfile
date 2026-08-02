#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM eclipse-temurin:17@sha256:068a8f9ae4b74d9a20de3ecca771ba1a6437f1d7a8a8ad6deaf9dbdd2274397a

ARG BENCHMARK_TARBALL

# Using ADD instead of COPY because ${BENCHMARK_TARBALL} is an archive that needs to be extracted
ADD ${BENCHMARK_TARBALL} /

RUN mv openmessaging-benchmark-* /benchmark

WORKDIR /benchmark

# Install vim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y vim tzdata && rm -rf /var/lib/apt/lists/*

# Start a shell by default
CMD ["/bin/bash"]
