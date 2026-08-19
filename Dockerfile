# // SPDX-License-Identifier: BUSL-1.1
# // Copyright (c) 2026 M. Javani
# //
# // This file is part of rzrouter.
# //
# // Use of this software is governed by the Business Source License 1.1
# // included in the LICENSE file in the root of this repository.


FROM ubuntu:24.04

RUN apt-get update && apt-get install -y ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Binary is copied to root by CI
COPY rzrouter /opt/rzrouter/rzrouter

RUN chmod +x /opt/rzrouter/rzrouter

EXPOSE 9000

CMD ["/opt/rzrouter/rzrouter"]