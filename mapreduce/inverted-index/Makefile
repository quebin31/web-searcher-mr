files = src/*.java
libs = lib/*.jar
binjar = bin/main.jar

ifeq ($(JAVA8), 1)
	hadoop_classpath = "${JAVA_HOME}/lib/tools.jar:$(libs)"
else 
	hadoop_classpath = "$(libs)"
endif

build: $(files) unjar
	HADOOP_CLASSPATH=$(hadoop_classpath) hadoop com.sun.tools.javac.Main -d classes $(files)
	mkdir bin
	jar cf $(binjar) -C classes . 

unjar:
	./unjar.sh $(libs)

clean: 
	@rm -rf classes 
	@rm -rf bin

run: $(binjar)
	@rm -rf output
	hadoop jar $(binjar) InvertedIndex input output
