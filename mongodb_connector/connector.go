package mongodb_connector

import (
	"context"
	"fmt"
	"net/url"

	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var logger *zap.Logger

const (
	DefaultHost           = "0.0.0.0"
	DefaultPort           = 27017
	DefaultDB             = ""
	DefaultUser           = ""
	DefaultPassword       = ""
	DefaultAuthMechanism  = ""
	DefaultReadPreference = ""
	DefaultSSLMode        = false
)

type MongoDBConnector struct {
	params Params
	logger *zap.Logger
	client *mongo.Client
	scope  string
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	var c *MongoDBConnector

	return fx.Module(
		scope,
		fx.Provide(func(p Params) *MongoDBConnector {

			logger = p.Logger.Named(scope)

			rc := &MongoDBConnector{
				params: p,
				logger: logger,
				scope:  scope,
			}

			rc.initDefaultConfigs()

			return rc
		}),
		fx.Populate(&c),
		fx.Invoke(func(p Params) *MongoDBConnector {

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

func (c *MongoDBConnector) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", c.scope, key)
}

func (c *MongoDBConnector) initDefaultConfigs() {
	viper.SetDefault(c.getConfigPath("host"), DefaultHost)
	viper.SetDefault(c.getConfigPath("port"), DefaultPort)
	viper.SetDefault(c.getConfigPath("db"), DefaultDB)
	viper.SetDefault(c.getConfigPath("user"), DefaultUser)
	viper.SetDefault(c.getConfigPath("password"), DefaultPassword)
	viper.SetDefault(c.getConfigPath("authMechanism"), DefaultAuthMechanism)
	viper.SetDefault(c.getConfigPath("readPreference"), DefaultReadPreference)
	viper.SetDefault(c.getConfigPath("sslmode"), DefaultSSLMode)
}

func (c *MongoDBConnector) onStart(ctx context.Context) error {

	host := viper.GetString(c.getConfigPath("host"))
	port := viper.GetInt(c.getConfigPath("port"))
	user := viper.GetString(c.getConfigPath("user"))
	password := viper.GetString(c.getConfigPath("password"))

	data := map[string]string{
		"tls": "false",
	}

	// Create a url.Values object
	values := url.Values{}

	if viper.GetBool(c.getConfigPath("sslmode")) {
		data["tls"] = "true"
	}

	if viper.GetString(c.getConfigPath("authMechanism")) != "" {
		data["authMechanism"] = viper.GetString(c.getConfigPath("authMechanism"))
	}

	if viper.GetString(c.getConfigPath("db")) != "" {
		data["authSource"] = viper.GetString(c.getConfigPath("db"))
	}

	if viper.GetString(c.getConfigPath("readPreference")) != "" {
		data["readPreference"] = viper.GetString(c.getConfigPath("readPreference"))
	}

	// Add data to the values object
	for key, value := range data {
		values.Add(key, value)
	}

	// Encode the values to URL-encoded string
	encodedString := values.Encode()

	uriData := fmt.Sprintf("%v:%v/?%v", host, port, encodedString)
	if user != "" && password != "" {
		uriData = fmt.Sprintf("%v:%v@%v", user, password, uriData)
	}

	uri := fmt.Sprintf("mongodb://%v", uriData)

	c.logger.Info("Starting MongoDBConnector",
		zap.String("clientOptions", uri),
	)

	// Set client options
	clientOptions := options.Client().ApplyURI(uri)

	// Connect to MongoDB
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		c.logger.Error(err.Error())
		return err
	}

	// Check the connection
	if err := client.Ping(context.TODO(), nil); err != nil {
		c.logger.Error(err.Error())
		return err
	}

	c.client = client

	return nil
}

func (c *MongoDBConnector) onStop(ctx context.Context) error {

	c.logger.Info("Stopped MongoDBConnector")

	return c.client.Disconnect(context.TODO())
}

func (c *MongoDBConnector) GetClient() *mongo.Client {
	return c.client
}
