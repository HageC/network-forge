FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /networkforge-api ./cmd/api
FROM alpine:3.22
COPY --from=build /networkforge-api /networkforge-api
EXPOSE 8080
CMD ["/networkforge-api"]
