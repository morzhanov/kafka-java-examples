# Task 3 results (Connect)

1. Created base app for FileSink connector. Added FileSinkConnectorConfig

   ![Alt text](./screenshots/01.png?raw=true)

2. Added FileSinkConnector

   ![Alt text](./screenshots/02.png?raw=true)

3. Added FileSinkConnectorTask

    ![Alt text](./screenshots/03.png?raw=true)

4. Copied project to the kafka broker docker container via command
    `docker cp . <container-id>:/home/appuser/connector`

    ![Alt text](./screenshots/04.png?raw=true)

5. Created `fsc.properties` file for FileSink connector

    ![Alt text](./screenshots/05.png?raw=true)

6. Created `fsc-worker.properties` file for FileSink worker

    ![Alt text](./screenshots/06.png?raw=true)

7. On borker container started kafka connect connector via command:
    `sh /bin/connect-standalone /home/appuser/connector/config/fsc-worker.properties /home/appuser/connector/config/fsc.properties`

    ![Alt text](./screenshots/07.png?raw=true)

8. Fetched data from GitHub API and pushed to kafka topic (app from previous task)

    ![Alt text](./screenshots/08.png?raw=true)

9. Kafka Connect worker successfully started

    ![Alt text](./screenshots/09.png?raw=true)

10. Connector saved events to the `events.txt` file

    ![Alt text](./screenshots/10.png?raw=true)
