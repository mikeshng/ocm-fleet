// Command enrollment-agent presents a cluster to an OCM Fleet resource
// manager. It is placed on a cluster by an enrollment, next to the
// access service account whose token it presents.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/stolostron/ocm-fleet/server/internal/enrollmentagent"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(os.Args[1:], logger); err != nil {
		logger.Error("enrollment agent stopped", "error", err)
		os.Exit(1)
	}
}

func run(args []string, logger *slog.Logger) error {
	var cfg enrollmentagent.Config
	flags := flag.NewFlagSet("enrollment-agent", flag.ContinueOnError)
	flags.StringVar(&cfg.ServerURL, "server-url", "", "https address of the resource manager")
	flags.StringVar(&cfg.ServerCAFile, "server-ca-file", "", "PEM file with the CA of the resource manager's certificate (default: system roots)")
	flags.StringVar(&cfg.ConnectAddress, "connect-address", "", "host:port to dial instead of resolving the host of --server-url")
	flags.StringVar(&cfg.BundleTokenFile, "bundle-token-file", "", "file holding the enrollment bundle's secret")
	flags.StringVar(&cfg.Namespace, "namespace", "", "namespace of the access service account (default: the agent's own)")
	flags.StringVar(&cfg.ServiceAccount, "service-account", "", "name of the access service account")
	flags.StringVar(&cfg.ClusterName, "cluster-name", "", "the cluster's name within the bundle's scope, for a bundle that covers several clusters")
	flags.DurationVar(&cfg.TokenLifetime, "token-lifetime", 0, "lifetime to ask for the access token (default 12h)")
	flags.StringVar(&cfg.HealthAddr, "health-addr", "", "address for /healthz and /readyz (default: not served)")
	flags.BoolVar(&cfg.Once, "once", false, "exit after the first successful presentation")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", flags.Arg(0))
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	agent, err := enrollmentagent.New(cfg, enrollmentagent.InCluster{}, logger)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	healthErr := make(chan error, 1)
	go func() { healthErr <- agent.ServeHealth(ctx) }()

	runErr := make(chan error, 1)
	go func() { runErr <- agent.Run(ctx) }()

	select {
	case err := <-runErr:
		cancel()
		if stopErr := <-healthErr; stopErr != nil {
			logger.Warn("health endpoint stopped with an error", "error", stopErr)
		}
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case err := <-healthErr:
		cancel()
		if stopErr := <-runErr; stopErr != nil && !errors.Is(stopErr, context.Canceled) {
			return errors.Join(err, stopErr)
		}
		return err
	}
}
