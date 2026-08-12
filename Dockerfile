
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Binary is copied to root by CI
COPY rzrouter /opt/rzrouter/rzrouter

RUN chmod +x /opt/rzrouter/rzrouter

EXPOSE 9000

CMD ["/opt/rzrouter/rzrouter"]