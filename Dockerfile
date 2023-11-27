ARG BASEIMAGE=swift:5.7.0
FROM ${BASEIMAGE}

WORKDIR /build

# First just resolve dependencies.
# This creates a cached layer that can be reused
# as long as your Package.swift/Package.resolved
# files do not change.
COPY ./Package.* ./
RUN swift package resolve

# Copy sources
COPY ./Sources ./Sources
COPY ./Tests ./Tests

# Build and test
RUN swift build -c debug
RUN swift test
