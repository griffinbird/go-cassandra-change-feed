package changefeed

import (
	"fmt"
	"log"
	"time"
	"github.com/griffinbird/go-cassandra-cf/inserter"
	"github.com/gocql/gocql"
)

const (
	watchQuery = "SELECT * FROM %s.%s where COSMOS_CHANGEFEED_START_TIME() = '%s'"
)

type ChangeFeed struct {
	table    string
	keyspace string
	session  *gocql.Session
}

func Create(table string, keyspace string, session *gocql.Session) ChangeFeed {
	return ChangeFeed{
		table:    table,
		keyspace: keyspace,
		session:  session,
	}
}

func (ins *ChangeFeed) MoveCity(id int ,city string) error {
	createTableQuery := "INSERT INTO %s.%s (user_id, user_bcity) VALUES (?,?)"
	query := ins.session.Query(fmt.Sprintf(createTableQuery, ins.keyspace, "city")).Bind(id, city)
	observer := inserter.CreateLocalObserver()
	iter := query.Observer(observer).Iter()
	requestCharge := inserter.GetRequestCharge(iter)
	err := iter.Close()
	if err != nil {
		return err
	}
	log.Printf("[ChangeFeed] Inserted city #%d (%s)                    [%.2f RUs in %.2fms]", id, city, requestCharge, observer.Milliseconds())
	return nil
}

func (ins *ChangeFeed) WatchChangeFeed() error {
	startTime := time.Now().UTC()
	pageState := []byte{}
	for {
		log.Println("[ChangeFeed] Starting query")
		// Build our query
		query := ins.session.Query(fmt.Sprintf(watchQuery, ins.keyspace, ins.table, startTime.Format(time.RFC3339)))

		// If we are continuing from where we were, set the page state
		if len(pageState) > 0 {
			query = query.PageState(pageState)
		}

		// Exec the query
		iter := query.Iter()

		// Store the page state for next time
		pageState = iter.PageState()

		// Read all of the changes and log them
		for {
			// An empty interface map is used to show all rows
			//  It is important that each row that we call MapScan() on is new
			row := map[string]interface{}{}
			if !iter.MapScan(row) {
				break
			}
			log.Printf("[ChangeFeed] Change detected: %v", row)
			err := ins.MoveCity(row["user_id"].(int),row["user_bcity"].(string))
			if err != nil {
				return err
			}

		}

		err := iter.Close()
		if err != nil {
			return err
		}
	}
}