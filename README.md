# tesla_modes_s

## Set-up instruction

It's a Spring Boot application which is using docker (docker compose) to host a dependencies, as:  
1 Kafka  
2 Redis  
3 Zookeeper    
  
to connec to to kafka, it use hostname: kafka. It mean **kafka** should be resolvable and point to **127.0.0.1** host. On Linux/MacOS it could be done via **/etc/hosts** file  

It was tested with **Java21** and requires **maven** to build and run.  
  
### Build guide
To build the application run: `mvn clean install`. It will include building the app and run unit/integration tests.  
  
### Run guide
1 `docker compose up -d zookeeper kafka kafka-ui redis` - it will start required Docker containers. **Please run this command from the project root folder**  
2 `mvn spring-boot:run -Dspring-boot.run.profiles=production` - to run the application. **Please run this command from the project root folder**  
3 Testing stage, it's possible to use **slightly modified from it origin** `data_generator.py` script


### Tear down
1 Stop the app. It's possible to do `CTRL + C` on Linux machine  
2 `docker compose down` - to tear down docker containers.  

## Edge cases comments
1 App provide `At least once` delivery guarantee, meaning some duplications possible. Consumer should be `idempotent`.  
2 Right now old events just dropped, no `Dead letter` queue in place  
3 To prevent evens lost, in case of app crashes, persistency layer (**Redis**) added. Used **AI** to generate some Radis code.
