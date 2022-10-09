## MVP for the news-aggregator

### Сгенерить JAR файл
$ mvn install -DskipTests

### Создание image Spring Boot
$ docker build -t news-aggregator.jar .

### Запуск контейнеров Spring Boot и PostgreSQL
$ docker-compose up -d

Можно тестировать API endpointы.

### Контракт API
![img_1.png](img_1.png)