//go:build !aws
// +build !aws

package main

import (
	"context"
	"github.com/ONSdigital/dp-search-api/config"
	"log"
	"os"
)

var Name = "development"

func getConfig(ctx context.Context) cliConfig {
	cfg, err := config.Get()
	if err != nil {
		log.Fatal(ctx, "error retrieving config", err)
		os.Exit(1)
	}

	return cliConfig{
		zebedeeURL:   "http://localhost:8082",
		esURL:        cfg.ElasticSearchAPIURL,
		signRequests: cfg.SignElasticsearchRequests,
	}
}
