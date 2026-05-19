FROM eclipse-temurin:21-jre-alpine

RUN apk add --no-cache graphviz gnuplot git bash wget
RUN wget -q https://github.com/jepsen-io/maelstrom/releases/download/v0.2.4/maelstrom.tar.bz2 \
    && tar xf maelstrom.tar.bz2 \
    && rm maelstrom.tar.bz2

WORKDIR /maelstrom
ENTRYPOINT ["./maelstrom"]
