files = src/*.java
libs = lib/*.jar
binjar = bin/main.jar

build: $(files) unjar
	HADOOP_CLASSPATH=$(libs) hadoop com.sun.tools.javac.Main -d classes $(files)
	mkdir bin
	jar --create --file $(binjar) -C classes . 

unjar:
	./unjar.sh $(libs)

clean: 
	@rm -rf classes 
	@rm -rf bin

run: $(binjar)
	@rm -rf output
	hadoop jar $(binjar) ${CLASS} input output