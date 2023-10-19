package main

import (
	"github.com/exoscale/sos-client-bucket-lifecycle/cmd"
)

var (
	bucket     string
	zone       string
	profile    string
	configPath string
)

func main() {
	cmd.CliExecute()
}
