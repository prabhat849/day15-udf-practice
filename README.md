
# Day 15 - UDF Practice

## Objective
Practice User-Defined Functions in Apache Spark using Scala.

## Topics Covered
- Create a Scala UDF
- Classify employee salary bands
- Add columns using withColumn
- Compare UDFs with built-in Spark functions
- Register a UDF for Spark SQL
- Classify customer transaction values

## Technologies
- Ubuntu / WSL
- Java 17
- Scala 2.12.18
- Apache Spark 3.5.9
- sbt

## Run the Project

```bash
export JAVA_TOOL_OPTIONS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
sbt compile
sbt run
```

## Output
The program output is saved in `output.txt`.

## Main Learning
Use built-in Spark functions when possible because Spark can often optimize them more effectively than custom UDFs.
