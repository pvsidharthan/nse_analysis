# MyDocument.md

## Map /tests to "jupyter" container

To segregate test code from "production code" map src/test -> /tests in the jupyter container as follows:

>
      jupyter:
        container_name: jupyter
        image: arjones/pyspark:2.4.5
        restart: always
        environment:
        MASTER: spark://master:7077
        depends_on:
        - master
        ports:
        - "8888:8888"
        volumes:
        - ./src/notebook:/notebook
        - ./src/utils:/utils
        - ./src/tests:/tests
        - ./dataset:/dataset

## Tests of UDF Functions

All UDF functions were tested [here](src/notebook/test%20UDF%20functions.ipynb)

