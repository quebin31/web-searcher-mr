files = src/*.java
libs = lib/*.jar

build: $(files) unjar
	HADOOP_CLASSPATH=$(libs) hadoop com.sun.tools.javac.Main -d classes $(files)
	mkdir bin
	jar --create --file bin/main.jar -C classes . 

unjar:
	./unjar.sh $(libs)

clean: 
	@rm -rf classes 
	@rm -rf bin

run: main.jar 
	@rm -rf output
	hadoop jar bin/main.jar PageRank input output