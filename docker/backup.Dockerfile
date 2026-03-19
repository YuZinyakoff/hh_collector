FROM postgres:16-alpine

RUN apk add --no-cache bash

COPY scripts/backup/backup_postgres.sh /usr/local/bin/backup_postgres.sh
COPY scripts/backup/restore_postgres.sh /usr/local/bin/restore_postgres.sh

RUN chmod +x /usr/local/bin/backup_postgres.sh /usr/local/bin/restore_postgres.sh

CMD ["/usr/local/bin/backup_postgres.sh"]
