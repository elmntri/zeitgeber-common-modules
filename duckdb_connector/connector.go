package duckdb_connector

import (
	"context"
	"fmt"
	"database/sql"
	"errors"

	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
	_ "github.com/marcboeker/go-duckdb"
)

var logger *zap.Logger

const (
	DefaultDataSource = "?access_mode=READ_WRITE"
)

type DuckDBConnector struct {
	params Params
	logger *zap.Logger
	db     *sql.DB
	scope  string
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	var c *DuckDBConnector

	return fx.Module(
		scope,
		fx.Provide(func(p Params) *DuckDBConnector {

			logger = p.Logger.Named(scope)

			rc := &DuckDBConnector{
				params: p,
				logger: logger,
				scope:  scope,
			}

			rc.initDefaultConfigs()

			return rc
		}),
		fx.Populate(&c),
		fx.Invoke(func(p Params) *DuckDBConnector {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: c.onStart,
					OnStop:  c.onStop,
				},
			)

			return c
		}),
	)
}

func (c *DuckDBConnector) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", c.scope, key)
}

func (c *DuckDBConnector) initDefaultConfigs() {
	viper.SetDefault(c.getConfigPath("dataSource"), DefaultDataSource)
}

func (c *DuckDBConnector) onStart(ctx context.Context) error {

	dataSource := viper.GetString(c.getConfigPath("dataSource"))

	db, err := sql.Open("duckdb", dataSource)
	if err != nil {
		c.logger.Error(err.Error())
		return err
	}

	if err := c.Check(db.Ping());err != nil {
		c.logger.Error(err.Error())
		return err
	}

	c.db = db

	return nil
}

func (c *DuckDBConnector) onStop(ctx context.Context) error {

	c.logger.Info("Stopped DuckDBConnector")

	return c.db.Close()
}

func (c *DuckDBConnector) Check(args ...interface{}) error {
	err := args[len(args)-1]
	if err != nil {
		return errors.New("Check Error")
	}

	return nil
}

func (c *DuckDBConnector) GetDB() *sql.DB {
	return c.db
}
