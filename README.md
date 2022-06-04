# Event stream processing
## Python + Kafka + Spark + PostgreSQL + Grafana

Application has three parts:
* EventGenerator.py
  * This part generates fake data, then sends it to kafka.
* Spark.py + EventReceiver.py
  * Spark.py reads data and aggregates it then sends it back to kafka on another topic.
  * EventReceiver.py reads computed data from kafka and writes it into Postgres.

## Run

Run commands from the project root directory.
### This run all services:
> docker-compose up -d --build

### Next run EventGenerator.py:
1. Log into docker event-generator CLI 
>docker exec -it <project_event-generator id> /bin/bash
2. Run in foreground: `export PYTHONPATH=$(pwd) && python -u ./app/EventGenerator.py  -m 100 -M 500`
3. Or in background: `export PYTHONPATH=$(pwd) &&nohup python -u ./app/EventGenerator.py -m 100 -M 500 &`

### Finally we can visualise our data:
Enter into `http://localhost:3000/` and log with `admin/admin`, there will be dashboard "Event Stream Processing".

![img.png](img.png)


