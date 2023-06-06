all:
	go build -o bin/parquet2gt .

clean:
	rm -rf bin/*
