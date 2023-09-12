ifndef DB
override DB = digi
endif

.PHONY: install run cmd db lamp_brightness
install:
	cp load $(GOPATH)/bin/
run:
	docker run -d --name timescaledb -p 5432:5432 \
	-e POSTGRES_PASSWORD=digi timescale/timescaledb:latest-pg12
cmd:
	PGPASSWORD=digi psql -h localhost -U postgres
db:
	PGPASSWORD=digi psql -h localhost -U postgres -c "CREATE DATABASE $(DB);"
lamp_brightness:
	PGPASSWORD=digi psql -h localhost -U postgres -d $(DB) < ./lamp_brightness.sql
