# MyDocument.md

# Map new folders to "jupyter" container

- To segregate test code from "production code" map src/test -> /tests 

In `docker-compose.yml` file change the jupyter container as follows and (re)start the containts

`docker compose -f ./docker-compose.yml --project-name my_assesment up`

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

# Data analysis

For data analysis click [data_analysis](src/notebook/data_analysis.ipynb)

For data wrangling / pre-processing click [pre_processing_and_wrangling](src/notebook/pre_processing_and_wrangling.ipynb)

## Tests of UDF Functions

All UDF functions were tested [here](src/notebook/test%20UDF%20functions.ipynb)

