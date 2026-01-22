package neo4j_connector

import (
	"context"
	"fmt"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var logger *zap.Logger

const (
	DefaultURI                     = "neo4j://localhost:7687"
	DefaultUsername                = "neo4j"
	DefaultPassword                = "password"
	DefaultDatabase                = "neo4j"
	DefaultMaxConnectionPoolSize   = 100
	DefaultConnectionTimeout       = 30 * time.Second
	DefaultMaxTransactionRetryTime = 30 * time.Second
	DefaultEncryption              = false
)

type Neo4jConnector struct {
	params Params
	logger *zap.Logger
	driver neo4j.DriverWithContext
	scope  string
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	var c *Neo4jConnector

	return fx.Module(
		scope,
		fx.Provide(func(p Params) *Neo4jConnector {

			logger = p.Logger.Named(scope)

			nc := &Neo4jConnector{
				params: p,
				logger: logger,
				scope:  scope,
			}

			nc.initDefaultConfigs()

			return nc
		}),
		fx.Populate(&c),
		fx.Invoke(func(p Params) *Neo4jConnector {

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

func (c *Neo4jConnector) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", c.scope, key)
}

func (c *Neo4jConnector) initDefaultConfigs() {
	viper.SetDefault(c.getConfigPath("uri"), DefaultURI)
	viper.SetDefault(c.getConfigPath("username"), DefaultUsername)
	viper.SetDefault(c.getConfigPath("password"), DefaultPassword)
	viper.SetDefault(c.getConfigPath("database"), DefaultDatabase)
	viper.SetDefault(c.getConfigPath("maxConnectionPoolSize"), DefaultMaxConnectionPoolSize)
	viper.SetDefault(c.getConfigPath("connectionTimeout"), DefaultConnectionTimeout)
	viper.SetDefault(c.getConfigPath("maxTransactionRetryTime"), DefaultMaxTransactionRetryTime)
	viper.SetDefault(c.getConfigPath("encryption"), DefaultEncryption)
}

func (c *Neo4jConnector) onStart(ctx context.Context) error {

	uri := viper.GetString(c.getConfigPath("uri"))
	username := viper.GetString(c.getConfigPath("username"))
	password := viper.GetString(c.getConfigPath("password"))
	maxConnectionPoolSize := viper.GetInt(c.getConfigPath("maxConnectionPoolSize"))
	connectionTimeout := viper.GetDuration(c.getConfigPath("connectionTimeout"))
	maxTransactionRetryTime := viper.GetDuration(c.getConfigPath("maxTransactionRetryTime"))
	encryption := viper.GetBool(c.getConfigPath("encryption"))

	c.logger.Info("Starting Neo4jConnector",
		zap.String("uri", uri),
		zap.String("username", username),
		zap.Int("maxConnectionPoolSize", maxConnectionPoolSize),
		zap.Duration("connectionTimeout", connectionTimeout),
		zap.Duration("maxTransactionRetryTime", maxTransactionRetryTime),
		zap.Bool("encryption", encryption),
	)

	config := func(conf *neo4j.Config) {
		conf.MaxConnectionPoolSize = maxConnectionPoolSize
		conf.ConnectionAcquisitionTimeout = connectionTimeout
		conf.MaxTransactionRetryTime = maxTransactionRetryTime
	}

	driver, err := neo4j.NewDriverWithContext(
		uri,
		neo4j.BasicAuth(username, password, ""),
		config,
	)
	if err != nil {
		c.logger.Error("Failed to create Neo4j driver", zap.Error(err))
		return err
	}

	if err := driver.VerifyConnectivity(ctx); err != nil {
		c.logger.Error("Failed to verify Neo4j connectivity", zap.Error(err))
		return err
	}

	c.driver = driver

	c.logger.Info("Neo4jConnector started successfully")

	return nil
}

func (c *Neo4jConnector) onStop(ctx context.Context) error {

	c.logger.Info("Stopping Neo4jConnector")

	if c.driver != nil {
		return c.driver.Close(ctx)
	}

	return nil
}

func (c *Neo4jConnector) GetDriver() neo4j.DriverWithContext {
	return c.driver
}

func (c *Neo4jConnector) GetSession(ctx context.Context, database ...string) neo4j.SessionWithContext {
	var dbName string

	if len(database) > 0 && database[0] != "" {
		dbName = database[0]
	} else {
		dbName = viper.GetString(c.getConfigPath("database"))
	}

	return c.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: dbName,
	})
}
