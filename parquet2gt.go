package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"time"

	gt "github.com/GreptimeTeam/greptimedb-client-go"
	"github.com/segmentio/parquet-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	options = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg = gt.NewCfg("127.0.0.1").
		WithPort(4001).         // default is 4001.
		WithDatabase("public"). // specify your database
		WithAuth("", "").
		WithDialOptions(options...). // specify your gRPC dail options
		WithCallOptions()            // specify your gRPC call options
)

type Row struct {
	Lts  time.Time `parquet:"lts,timestamp(microsecond)"`
	Rts  time.Time `parquet:"rts,timestamp(microsecond)"`
	Px   float64   `parquet:"px"`
	Qty  float64   `parquet:"qty"`
	Flag []byte    `parquet:"flag"`
}

func main() {
	var logLvlStr string

	flag.StringVar(&logLvlStr, "v", "info", "log level")
	flag.Parse()

	logCfg := zap.NewDevelopmentConfig()
	logCfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	var err error
	logCfg.Level, err = zap.ParseAtomicLevel(logLvlStr)
	if err != nil {
		panic(err)
	}

	logger, err := logCfg.Build()
	if err != nil {
		panic(err)
	}
	log := logger.Sugar()

	rows, err := parquet.ReadFile[Row](flag.Arg(0))
	if err != nil {
		panic(err)
	}

	schema := parquet.SchemaOf(new(Row))
	log.Info(schema)

	// insert(rows)
	streamInsert(rows)
}

func doInsert(client *gt.Client, table string, metric gt.Metric) {
	req := gt.InsertRequest{}
	req.WithTable(table).WithMetric(metric)
	reqs := gt.InsertsRequest{}
	reqs.Insert(req)

	fmt.Printf("ready to insert %d series\n", len(metric.GetSeries()))
	_, err := client.Insert(context.Background(), reqs)
	if err != nil {
		panic(err)
	}
}

func insert(rows []Row) {
	client, err := gt.NewClient(cfg)
	if err != nil {
		panic(err)
	}

	metric := gt.Metric{}
	metric.SetTimestampAlias("lts")
	metric.SetTimePrecision(time.Microsecond)

	table := fmt.Sprintf("trade_%d", rand.Intn(100000))
	fmt.Printf("table: %s\n", table)

	for i, c := range rows {
		// log.Debug(c)

		series := gt.Series{}
		series.SetTimestamp(c.Lts)
		series.AddField("rts", c.Rts)
		series.AddField("px", c.Px)
		series.AddField("qty", c.Qty)
		series.AddField("flag", string(c.Flag))
		metric.AddSeries(series)

		if i > 0 && i%5000 == 0 {
			doInsert(client, table, metric)
			metric = gt.Metric{}
			metric.SetTimestampAlias("lts")
			metric.SetTimePrecision(time.Microsecond)
		}
	}

	doInsert(client, table, metric)
}

func doStreamInsert(client *gt.StreamClient, table string, metric gt.Metric) {
	req := gt.InsertRequest{}
	req.WithTable(table).WithMetric(metric)
	reqs := gt.InsertsRequest{}
	reqs.Insert(req)

	fmt.Printf("ready to insert %d series\n", len(metric.GetSeries()))
	if err := client.Send(context.Background(), reqs); err != nil {
		fmt.Printf("err: %v\n", err)
		panic(err)
	}
}

func streamInsert(rows []Row) {
	client, err := gt.NewStreamClient(cfg)
	if err != nil {
		panic(err)
	}

	metric := gt.Metric{}
	metric.SetTimestampAlias("lts")
	metric.SetTimePrecision(time.Microsecond)

	table := fmt.Sprintf("trade_%d", rand.Intn(100000))
	fmt.Printf("table: %s\n", table)

	for i, c := range rows {
		// log.Debug(c)

		series := gt.Series{}
		series.SetTimestamp(c.Lts)
		series.AddField("rts", c.Rts)
		series.AddField("px", c.Px)
		series.AddField("qty", c.Qty)
		series.AddField("flag", string(c.Flag))
		metric.AddSeries(series)

		if i > 0 && i%5000 == 0 {
			doStreamInsert(client, table, metric)
			metric = gt.Metric{}
			metric.SetTimestampAlias("lts")
			metric.SetTimePrecision(time.Microsecond)
		}
	}
	doStreamInsert(client, table, metric)

	_, err = client.CloseAndRecv(context.Background())
	if err != nil {
		panic(err)
	}
}
