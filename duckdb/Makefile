TABLES = customer lineitem nation region orders part partsupp supplier


duckdb:
	# echo "INSTALL tpch; LOAD tpch; CALL dbgen(sf = 1);" | duckdb tpch.db

	mkdir -p data

	@for table in $(TABLES); do \
	  echo "Exporting $$table..."; \
	  duckdb tpch.db -c "COPY $$table TO 'data/$$table.tbl.tmp' (HEADER false, QUOTE '\"', FORCE_QUOTE *);"; \
	  header=$$(duckdb -csv tpch.db -c "DESCRIBE $$table;" | awk -F',' 'NR>1 { \
			t = tolower($$2); \
			if (t ~ /int/) t="int64()"; \
			else if (t ~ /varchar|text|char/) t="string()"; \
			else if (t ~ /decimal|numeric|double|real|float/) t="decimal()"; \
			else if (t ~ /date|timestamp/) t="date(2006-01-02)"; \
			print $$1 "." t  \
		}' | paste -sd',' -); \
	  echo $$header > data/$$table.tbl; \
	  cat data/$$table.tbl.tmp >> data/$$table.tbl; \
	  rm data/$$table.tbl.tmp; \
	done

# Example DB_HOST=127.0.0.1 DB_PORT=3306 DB_USER=root DB_PASS=root DB_NAME=tpch make import-mysql
import-mysql:
	@for table in $(TABLES); do \
	  echo "Importing $$table..."; \
	  mysql -h $(DB_HOST) -P $(DB_PORT) -u$(DB_USER) -p$(DB_PASS) $(DB_NAME) --local-infile=1 -e "\
	    TRUNCATE $$table; \
		LOAD DATA LOCAL INFILE 'data/$$table.csv' \
	    INTO TABLE $$table \
	    FIELDS TERMINATED BY ',' \
	    OPTIONALLY ENCLOSED BY '\"' \
	    LINES TERMINATED BY '\n' \
	    IGNORE 1 LINES;"; \
	done

# Example: DB_HOST=127.0.0.1 DB_PORT=27017 DB_USER=root DB_PASS=root DB_NAME=tpch make import-mongodb
import-mongodb:
	@for table in $(TABLES); do \
	  echo "Importing $$table into MongoDB..."; \
	  mongoimport --host $(DB_HOST) --port $(DB_PORT) \
	    --username $(DB_USER) --password $(DB_PASS) \
	    --authenticationDatabase admin \
	    --db $(DB_NAME) --collection $$table \
		--columnsHaveTypes \
	    --type csv --file data/$$table.tbl --headerline --drop; \
	done
