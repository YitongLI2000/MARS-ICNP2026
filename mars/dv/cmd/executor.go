package cmd

import (
	"fmt"

	"github.com/named-data/ndnd/dv/config"
	"github.com/named-data/ndnd/dv/dv"
	"github.com/named-data/ndnd/std/engine"
	"github.com/named-data/ndnd/std/ndn"
)

type DvExecutor struct {
	engine ndn.Engine
	router *dv.Router
}

func NewDvExecutor(config *config.Config) (*DvExecutor, error) {
	dve := new(DvExecutor)

	// Validate configuration sanity
	err := config.Parse()
	if err != nil {
		return nil, fmt.Errorf("failed to validate dv config: %w", err)
	}

	// Start NDN engine
	dve.engine = engine.NewBasicEngine(engine.NewDefaultFace())

	// Create the DV router
	dve.router, err = dv.NewRouter(config, dve.engine)
	if err != nil {
		return nil, fmt.Errorf("failed to create dv router: %w", err)
	}

	return dve, nil
}

func (dve *DvExecutor) Start() {
	err := dve.engine.Start()
	if err != nil {
		panic(fmt.Errorf("failed to start dv engine: %w", err))
	}
	defer dve.engine.Stop()

	err = dve.router.Start() // blocks forever
	if err != nil {
		panic(fmt.Errorf("failed to start dv router: %w", err))
	}
}

func (dve *DvExecutor) Stop() {
	dve.router.Stop()
}

func (dve *DvExecutor) Router() *dv.Router {
	return dve.router
}
