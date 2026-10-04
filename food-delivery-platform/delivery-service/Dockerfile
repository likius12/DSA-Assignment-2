FROM ballerina/ballerina:2201.13.4 AS build
WORKDIR /home/ballerina
COPY . .
RUN bal build --observability-included

FROM ballerina/ballerina:2201.13.4
WORKDIR /home/ballerina
COPY --from=build /home/ballerina/target/bin/delivery_service.jar .
COPY Config.toml .                    
EXPOSE 8085
CMD ["bal", "run", "delivery_service.jar"]