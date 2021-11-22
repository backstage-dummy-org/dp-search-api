//go:build aws
// +build aws

package main

import "context"

var Name = "aws"

func getConfig(ctx context.Context) cliConfig {
	return cliConfig{
		zebedeeURL:   "http://localhost:10050",
		esURL:        "https://blah-blah-blah.eu-west-1.es.amazonaws.com",
		signRequests: true,
	}
}
