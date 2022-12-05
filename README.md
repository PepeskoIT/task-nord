# nord-task

## Name
Meta file analysis.

## Description
Simple analysis and processing service to visiualise OOP and python skills.

## Installation
Docker with compose plugin.

## Usage
While in docker/ folder you can do:
```
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d
```

## Authors
Lukasz Przybyl - joboffers@pepesko.eu

## License
Copyright 2022, Lukasz Przybyl, All rights reserved.

## Project status
First iteration. TODO: add tests.

## High level UML
![High level UML](https://cloud.pepesko.eu/index.php/s/FMt3McbnFZXddEq/download/nord_inner_export.drawio.png "High level UML")

## Frameworks implementation benchmark results
Measured executions times depending on choosen files pool and implentation framework.
Pyspark is a clear winner performance wise. Almost 4x faster then previous async implementation.

| framework implementation | N_NUMBER of files to process  | Measured execution time (h:min:s)|
| --- | --- | --- |
| sync | 2000 | 00:12:36 |
| async | 2000 | 00:03:33 |
| pyspark | 2000 | 00:01:04 |
| sync | 20000 | N/A|
| async | 20000 | 00:38:32 |
| pyspark | 20000 |  00:11:13 |
