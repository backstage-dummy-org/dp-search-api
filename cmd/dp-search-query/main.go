package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	rchttp "github.com/ONSdigital/dp-rchttp"
	"github.com/ONSdigital/dp-search-query/api"
	"github.com/ONSdigital/dp-search-query/config"
	"github.com/ONSdigital/dp-search-query/elasticsearch"
	"github.com/ONSdigital/go-ns/log"
)

func main() {
	log.Namespace = "dp-search-query"

	cfg, err := config.Get()
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	// sensitive fields are omitted from config.String().
	log.Info("config on startup", log.Data{"config": cfg})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	apiErrors := make(chan error, 1)

	elasticSearchClient := elasticsearch.New(cfg.ElasticSearchAPIURL, rchttp.NewClient())
	if err := api.CreateAndInitialise(cfg.BindAddr, elasticSearchClient, apiErrors); err != nil {
		log.ErrorC("Error initialising API", err, nil)
		os.Exit(1)
	}

	gracefulShutdown := func() {
		log.Info("Commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": cfg.GracefulShutdownTimeout})
		ctx, cancel := context.WithTimeout(context.Background(), cfg.GracefulShutdownTimeout)

		// stop any incoming requests before closing any outbound connections
		api.Close(ctx)

		log.Info("shutdown complete", nil)
		cancel()
	}

	// blocks until a fatal error occurs
	select {
	case err := <-apiErrors:
		log.ErrorC("search-query api error received", err, nil)
	case <-signals:
		log.Debug("os signal received", nil)
		gracefulShutdown()
	}

	// Gracefully shutdown the application closing any open resources.

	os.Exit(1)
}
