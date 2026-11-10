FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS TARGETARCH VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/railway-forwarder .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/railway-forwarder /railway-forwarder
USER nonroot:nonroot
ENTRYPOINT ["/railway-forwarder"]
