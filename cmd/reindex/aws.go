//go:build aws
// +build aws

package main

import "context"

var Name = "aws"

func getConfig(ctx context.Context) cliConfig {
	return cliConfig{
		zebedeeURL:   "http://localhost:10050",
		esURL:        "https://vpc-develop-site-54e7wtwq3uhfsardcmb5umiyrm.eu-west-1.es.amazonaws.com",
		signRequests: true,
	}
}
