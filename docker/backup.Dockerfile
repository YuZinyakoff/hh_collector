FROM postgres:16-alpine

WORKDIR /app

CMD ["sh", "-c", "echo backup scaffold"]
