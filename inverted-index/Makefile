files = src/*.java
libs = lib/*.jar

build: $(files) unjar
	HADOOP_CLASSPATH=$(libs) hadoop com.sun.tools.javac.Main -d classes $(files)
	jar --create --file main.jar -C classes . 

unjar:
	./unjar.sh $(libs)

clean: 
	@rm -rf classes 
	@rm -rf main.jar

run: main.jar 
	@rm -rf output
	hadoop jar main.jar InvertedIndex input output