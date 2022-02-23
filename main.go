package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-search-api/config"
	"github.com/ONSdigital/dp-search-api/service"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/pkg/errors"
)

const serviceName = "dp-search-api"

var (
	BuildTime="1645621695"
	GitCommit="d671d2ff50963158cc204463ca0116702194a7a9"
	Version=""
)

func main() {
	log.Namespace = serviceName
	ctx := context.Background()

	if err := run(ctx); err != nil {
		log.Error(ctx, "application unexpectedly failed", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Create the service, providing an error channel for fatal errors
	svcErrors := make(chan error, 1)
	svcList := service.NewServiceList(&service.Init{})

	// Read config
	cfg, err := config.Get()
	if err != nil {
		log.Fatal(ctx, "error retrieving Config", err)
		return err
	}

	log.Info(ctx, "config on startup", log.Data{"config": cfg, "build_time": BuildTime, "git-commit": GitCommit})

	// Run the service
	svc, err := service.Run(ctx, cfg, svcList, BuildTime, GitCommit, Version, svcErrors)
	if err != nil {
		return errors.Wrap(err, "running service failed")
	}

	// Blocks until a fatal error occurs
	select {
	case err := <-svcErrors:
		log.Fatal(ctx, "search api error received", err)
	case <-signals:
		log.Info(ctx, "os signal received")
	}

	return svc.Close(ctx)
}
