all:
	go build -o bin/kafka2gt .

clean:
	rm -rf bin/*
