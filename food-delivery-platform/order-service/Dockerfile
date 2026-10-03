FROM ballerina/ballerina:2201.13.4 AS build
WORKDIR /home/ballerina
COPY . .
RUN bal build

FROM ballerina/ballerina:2201.13.4
WORKDIR /home/ballerina
COPY --from=build /home/ballerina/target/bin/order_service.jar .
EXPOSE 8083
CMD ["bal", "run", "order_service.jar"]