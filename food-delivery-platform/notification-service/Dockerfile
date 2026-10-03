FROM ballerina/ballerina:2201.13.4 AS build
WORKDIR /home/ballerina
COPY . .
RUN bal build

FROM ballerina/ballerina:2201.13.4
WORKDIR /home/ballerina
COPY --from=build /home/ballerina/target/bin/notification_service.jar .
EXPOSE 8086
CMD ["bal", "run", "notification_service.jar"]