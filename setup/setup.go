package setup

import (
	"fmt"
	"log"

	"github.com/gocql/gocql"
)

const (
	dropKeyspaceQuery   = "DROP KEYSPACE IF EXISTS %s"
	createKeyspaceQuery = "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 }"
	createTableQuery    = "CREATE TABLE %s.%s (user_id int PRIMARY KEY, user_name text, user_bcity text)"
	createCityTableQuery    = "CREATE TABLE %s.%s (user_id int PRIMARY KEY, user_bcity text)"
)

func SetupTables(session *gocql.Session) (string, string, error) {
	keyspace := "demo"
	table := "user"

	log.Println("[Setup] Dropping keyspace...")
	err := dropKeySpaceIfExists(keyspace, session)
	if err != nil {
		return table, keyspace, err
	}

	log.Println("[Setup] Creating keyspace...")
	err = createKeySpace(keyspace, session)
	if err != nil {
		return table, keyspace, err
	}

	log.Println("[Setup] Creating table...")
	err = createTable(table, keyspace, session)
	if err != nil {
		return table, keyspace, err
	}
	err = createCityTable("city", keyspace, session)
	return table, keyspace, err
}

func dropKeySpaceIfExists(keyspace string, session *gocql.Session) error {
	return session.Query(fmt.Sprintf(dropKeyspaceQuery, keyspace)).Exec()
}

func createKeySpace(keyspace string, session *gocql.Session) error {
	return session.Query(fmt.Sprintf(createKeyspaceQuery, keyspace)).Exec()
}

func createTable(table string, keyspace string, session *gocql.Session) error {
	return session.Query(fmt.Sprintf(createTableQuery, keyspace, table)).Exec()
}

func createCityTable(table string, keyspace string, session *gocql.Session) error {
	return session.Query(fmt.Sprintf(createCityTableQuery, keyspace, table)).Exec()
}