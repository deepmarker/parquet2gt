package main

import (
	"context"
	"flag"
	"time"

	gt "github.com/GreptimeTeam/greptimedb-client-go"
	"github.com/segmentio/parquet-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Row struct {
	Lts  time.Time `parquet:"lts,timestamp(microsecond)"`
	Rts  time.Time `parquet:"rts,timestamp(microsecond)"`
	Px   float64   `parquet:"px"`
	Qty  float64   `parquet:"qty"`
	Flag []uint8   `parquet:"flag"`
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

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cfg := gt.NewCfg("127.0.0.1").
		WithPort(4001).         // default is 4001.
		WithDatabase("public"). // specify your database
		WithAuth("", "").
		WithDialOptions(options...). // specify your gRPC dail options
		WithCallOptions()            // specify your gRPC call options

	client, err := gt.NewStreamClient(cfg)
	if err != nil {
		panic(err)
	}

	reqs := gt.InsertsRequest{}
	metric := gt.Metric{}
	metric.SetTimestampAlias("lts")
	metric.SetTimePrecision(time.Microsecond)
	for i, c := range rows {
		log.Debug(c)

		series := gt.Series{}
		series.SetTimestamp(c.Lts)
		series.AddField("rts", c.Rts)
		series.AddField("px", c.Px)
		series.AddField("qty", c.Qty)
		series.AddField("flag", c.Flag)
		metric.AddSeries(series)
		req := gt.InsertRequest{}
		req.WithTable("trade2").WithMetric(metric)
		reqs.Insert(req)

		if i%100 == 0 {
			if err = client.Send(context.Background(), reqs); err != nil {
				panic(err)
			}
		}

	}
	_, err = client.CloseAndRecv(context.Background())
	if err != nil {
		panic(err)
	}
}
