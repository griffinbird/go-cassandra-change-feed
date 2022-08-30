package main

import (
	"crypto/tls"
	"log"
	"os"
	"sync"
	"time"

	"github.com/griffinbird/go-cassandra-cf/changefeed"
	"github.com/griffinbird/go-cassandra-cf/inserter"
	"github.com/griffinbird/go-cassandra-cf/setup"

	"github.com/gocql/gocql"
	"gopkg.in/yaml.v2"
)

const (
	configFileName = "config.yml"
)

type Config struct {
	Host     string `yaml:"Host"`
	Port     int    `yaml:"Port"`
	Username string `yaml:"Username"`
	Password string `yaml:"Password"`
}

func main() {
	// Load the config from config.yml
	config, err := loadConfig(configFileName)
	if err != nil {
		log.Fatalf("Failed to read config file '%s': %v", configFileName, err)
	}

	// Set up a connection/session to Cassandra
	session, err := connect(config)
	if err != nil {
		log.Fatal("Failed to connect to DB: ", err)
	}

	// Create our keyspace/table
	table, keyspace, err := setup.SetupTables(session)
	if err != nil {
		log.Fatal("Failed to setup DB: ", err)
	}

	// Set up our tasks
	insert := inserter.Create(table, keyspace, session)
	feed := changefeed.Create(table, keyspace, session)

	// Kick off the change feed monitoring and inserting data using a helper
	goWhenAny(
		insert.InsertRecords,
		feed.WatchChangeFeed,
	)
}

// Helper function to connect to a Cassandra cluster
func connect(config Config) (*gocql.Session, error) {
	clusterConfig := gocql.NewCluster(config.Host)
	clusterConfig.Port = config.Port
	clusterConfig.ProtoVersion = 4
	clusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: config.Username, Password: config.Password}
	clusterConfig.SslOpts = &gocql.SslOptions{Config: &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}}
	clusterConfig.ConnectTimeout = 10 * time.Second
	clusterConfig.Timeout = 10 * time.Second
	clusterConfig.DisableInitialHostLookup = true
	return clusterConfig.CreateSession()
}

// Helper function to load config from a file
func loadConfig(path string) (Config, error) {
	var config Config
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_SYNC, 0)
	if err != nil {
		return config, err
	}
	defer f.Close()
	err = yaml.NewDecoder(f).Decode(&config)
	return config, err
}

// Helper function to allow you to run multiple funcs as goroutines
//  Will return when any of them have completed
func goWhenAny(funcs ...func() error) {
	var wg sync.WaitGroup
	wg.Add(1)
	for i := 0; i < len(funcs); i++ {
		go func(f func() error) {
			err := f()
			wg.Done()
			if err != nil {
				log.Fatalln("Error: ", err)
			}
		}(funcs[i])
	}
	wg.Wait()
}