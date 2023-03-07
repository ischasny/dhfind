package main

import (
	"context"
	"flag"
	"os"
	"os/signal"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ischasny/dhfind"
)

var (
	log = logging.Logger("cmd/daemon")
)

func main() {
	listenAddr := flag.String("listenAddr", "0.0.0.0:40080", "The dhstore HTTP server listen address.")
	dhstoreAddr := flag.String("dhstoreAddr", "", "The dhstore HTTP address.")
	llvl := flag.String("logLevel", "info", "The logging level. Only applied if GOLOG_LOG_LEVEL environment variable is unset.")

	flag.Parse()

	if _, set := os.LookupEnv("GOLOG_LOG_LEVEL"); !set {
		_ = logging.SetLogLevel("*", *llvl)
	}

	if *listenAddr == "" || *dhstoreAddr == "" {
		panic("listen and dhstore addresses must be provided")
	}

	server, err := dhfind.NewHttpServer(*listenAddr, *dhstoreAddr)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err := server.Start(ctx); err != nil {
		panic(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Info("Terminating...")
	if err := server.Shutdown(ctx); err != nil {
		log.Warnw("Failure occurred while shutting down server.", "err", err)
	} else {
		log.Info("Shut down server successfully.")
	}
}
