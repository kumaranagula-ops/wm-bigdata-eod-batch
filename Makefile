# Wealth Management EOD batch: common commands. Run `make help`.
SHELL := /bin/bash
AETHER ?= $(HOME)/aether_trade_project
IMAGE  ?= aether-bigdata:spark3.5-hadoop3.3
DATE   ?= 2026-11-09

# The aether stack (HDFS, YARN, Hive, Spark, Airflow) plus this project's Airflow mounts.
AETHER_COMPOSE = WM_HOME=$(CURDIR) docker compose --project-directory $(AETHER) \
	-f $(AETHER)/docker-compose.yml -f $(AETHER)/docker-compose.bigdata.yml \
	-f $(CURDIR)/docker-compose.airflow-override.yml
WM_COMPOSE = docker compose -f $(CURDIR)/docker-compose.yml
AIRFLOW = $(AETHER_COMPOSE) exec -T airflow-scheduler airflow
BEELINE = docker exec -i aether_trade_project-hiveserver2-1 beeline -u jdbc:hive2://localhost:10000 --silent=true --showHeader=true --outputformat=table

.PHONY: help env data build up down wm-down airflow-setup topic crm-changes kafka-produce \
        trigger day status test yarn-logs beeline beeline-report seed-s3 snowflake-setup clean

help:  ## list targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

env:  ## create .env from .env.example (once)
	@test -f .env || (cp .env.example .env && chmod 600 .env && echo "created .env - edit passwords")

data:  ## generate synthetic CRM seed, NAS feeds, market data, Kafka events
	python3 scripts/generate_data.py

build:  ## package wm_pipeline into dist/ (shipped to YARN with --py-files)
	@# Replace files, never the directory itself: Airflow bind-mounts dist/, and a
	@# deleted-and-recreated directory shows up empty inside the container.
	@mkdir -p dist && rm -f dist/wm_pipeline.zip.tmp
	@zip -qr dist/wm_pipeline.zip.tmp wm_pipeline -x '*__pycache__*' '*.pyc' && mv -f dist/wm_pipeline.zip.tmp dist/wm_pipeline.zip
	@cp jobs/run.py dist/run.py
	@echo "built dist/wm_pipeline.zip ($$(du -h dist/wm_pipeline.zip | cut -f1))"

up: env build  ## start aether stack (+ WM mounts), MySQL and Kafka, then pool and topic
	@test -f sql/mysql/init/02_seed.sql || $(MAKE) data
	$(AETHER_COMPOSE) up -d
	$(WM_COMPOSE) up -d --wait
	$(MAKE) topic airflow-setup

down:  ## stop WM containers and the aether stack (data volumes kept)
	$(WM_COMPOSE) stop
	$(AETHER_COMPOSE) stop

wm-down:  ## stop only MySQL and Kafka
	$(WM_COMPOSE) stop

airflow-setup:  ## create the wm_yarn pool (1 slot = one Spark app on the 4 GB YARN node)
	$(AIRFLOW) pools set wm_yarn 1 "WM Spark jobs on YARN"
	$(AIRFLOW) dags list 2>/dev/null | grep -E 'wm_eod_batch|dag_id' || echo "DAG not parsed yet - wait 30s and run: make airflow-setup"

topic:  ## create the Kafka topic (3 partitions, keyed by account)
	docker exec wm-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server wm-kafka:9092 \
		--create --if-not-exists --topic wm.intraday.trades --partitions 3 --replication-factor 1

crm-changes:  ## apply day-2 CRM changes in MySQL (FA leaves, book reassigned, new client)
	docker exec wm-mysql sh -c 'mysql -u root -p"$$MYSQL_ROOT_PASSWORD" < /changes/20261109_changes.sql'
	@echo "applied CRM changes; the next batch picks them up through the watermark"

kafka-produce:  ## publish intraday OMS trade events for DATE (e.g. make kafka-produce DATE=2026-11-09)
	@name=intraday_trades_$$(echo $(DATE) | tr -d -).jsonl; \
	test -f data/kafka/$$name || (echo "no events file data/kafka/$$name" && exit 1); \
	docker exec wm-kafka sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server wm-kafka:9092 \
		--topic wm.intraday.trades --property parse.key=true --property key.separator='|' < /events/$$name" && \
	echo "published $$(wc -l < data/kafka/$$name | tr -d ' ') events for $(DATE)"

trigger:  ## trigger the DAG for DATE
	$(AIRFLOW) dags unpause wm_eod_batch
	$(AIRFLOW) dags trigger wm_eod_batch --conf '{"business_date": "$(DATE)"}'

day:  ## demo helper: produce Kafka events for DATE then trigger the batch
	@if [ "$(DATE)" != "2026-11-06" ]; then $(MAKE) kafka-produce DATE=$(DATE); fi
	$(MAKE) trigger DATE=$(DATE)

status:  ## latest run_control rows and DQ failures from Hive
	@echo "SELECT business_date, job_name, status, rows_in, rows_out, rows_rejected, substr(message,1,60) AS message FROM wm_audit.run_control ORDER BY business_date DESC, started_at DESC LIMIT 25;" | $(BEELINE)
	@echo "SELECT business_date, job_name, check_name, severity, failed_count, status FROM wm_audit.dq_results WHERE status <> 'PASS' ORDER BY business_date DESC LIMIT 25;" | $(BEELINE)

yarn-logs:  ## driver log lines of the latest YARN app for JOB (e.g. make yarn-logs JOB=gold_book_of_record)
	@docker exec aether_trade_project-resourcemanager-1 bash -c 'app=$$(yarn application -list -appStates FINISHED,FAILED,KILLED 2>/dev/null | grep "wm_$(JOB)_" | sort | tail -1 | cut -f1); \
	echo "== $$app"; yarn logs -applicationId $$app 2>/dev/null | grep -E "\[wm\.|Error|Exception|DQ " | head -200'

beeline-report:  ## run the HiveQL reporting file in Hive itself for DATE (slow: Hive on MapReduce)
	docker cp wm_pipeline/resources/hive/gold_reporting.hql aether_trade_project-hiveserver2-1:/tmp/gold_reporting.hql
	docker exec aether_trade_project-hiveserver2-1 beeline -u jdbc:hive2://localhost:10000 \
		--hivevar as_of_date=$(DATE) -f /tmp/gold_reporting.hql

beeline:  ## open an interactive beeline session
	docker exec -it aether_trade_project-hiveserver2-1 beeline -u jdbc:hive2://localhost:10000

test:  ## unit tests (pytest + local SparkSession) inside the cluster image
	docker run --rm -v $(CURDIR):/opt/wm -w /opt/wm --entrypoint bash $(IMAGE) -c \
		"python3 -m pip install -q --disable-pip-version-check pytest==7.4.4 2>&1 | tail -1; \
		 PYTHONPATH=/opt/wm:/opt/spark/python:\$$(ls /opt/spark/python/lib/py4j-*.zip) python3 -m pytest -q -p no:cacheprovider tests"

seed-s3: build  ## upload data/market to s3://$WM_S3_BUCKET (needs WM_S3_BUCKET in .env and ~/.aws)
	@set -a; source .env; set +a; test -n "$$WM_S3_BUCKET" || (echo "set WM_S3_BUCKET in .env" && exit 1); \
	eval $$(python3 -c 'import configparser,os;c=configparser.ConfigParser();c.read(os.path.expanduser("~/.aws/credentials"));d=c["default"];print(f"export AWS_ACCESS_KEY_ID={d[\"aws_access_key_id\"]} AWS_SECRET_ACCESS_KEY={d[\"aws_secret_access_key\"]}")'); \
	docker run --rm --network aether-bigdata -v $(CURDIR):/opt/wm -v wm_ivy:/root/.ivy2 \
		-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e WM_S3_BUCKET -e WM_S3_PREFIX \
		--entrypoint /opt/spark/bin/spark-submit $(IMAGE) --master 'local[2]' \
		--packages org.apache.hadoop:hadoop-aws:3.3.4 \
		--conf spark.hadoop.fs.s3a.endpoint=s3.$$WM_AWS_REGION.amazonaws.com \
		--py-files /opt/wm/dist/wm_pipeline.zip /opt/wm/jobs/run.py \
		--job seed_market_s3 --business-date $(DATE) --config /opt/wm/conf/pipeline.json

snowflake-setup:  ## create WM_DB.GOLD tables in Snowflake (uses the `wm` snow CLI connection)
	snow sql -c wm -f sql/snowflake/01_setup.sql

clean:  ## remove generated data and build output (keeps Docker volumes)
	rm -rf dist nas/inbound data/kafka data/market sql/mysql/init/02_seed.sql sql/mysql/changes
