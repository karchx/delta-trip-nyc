.PHONY: build
build:
	sbt clean assembly

.PHONY: run
run:
	spark-submit --class com.deltaTripNyc.DeltaTripNyc --master spark://spark-master:7077 target/scala-2.12/data-pipeline-assembly-0.1.0.jar
