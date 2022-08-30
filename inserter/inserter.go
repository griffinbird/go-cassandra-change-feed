package inserter

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/gocql/gocql"
)

const (
	createQuery = "INSERT INTO %s.%s (user_id, user_name , user_bcity) VALUES (?,?,?)"
)

type Inserter struct {
	table    string
	keyspace string
	session  *gocql.Session
}

func Create(table string, keyspace string, session *gocql.Session) Inserter {
	return Inserter{
		table:    table,
		keyspace: keyspace,
		session:  session,
	}
}

var cities = []string{"New Delhi", "New York", "Bangalore", "Seattle"}

func (ins *Inserter) InsertRecords() error {
	for id := 1; ; id++ {
		err := ins.insertRow(id)
		if err != nil {
			return err
		}

		// Sleep between each insert
		time.Sleep(time.Second * 5)
	}
}

func (ins *Inserter) insertRow(id int) error {
	// Assemble our data
	name := "user-" + strconv.Itoa(id)
	city := cities[rand.Intn(len(cities))]

	// Build the query and execute
	query := ins.session.Query(fmt.Sprintf(createQuery, ins.keyspace, ins.table)).Bind(id, name, city)
	observer := CreateLocalObserver()
	iter := query.Observer(observer).Iter()
	requestCharge := GetRequestCharge(iter)
	err := iter.Close()
	if err != nil {
		return err
	}

	log.Printf("[Inserter] Inserted user #%d (%s)                    [%.2f RUs in %.2fms]", id, name, requestCharge, observer.Milliseconds())
	return nil
}

// Helpers for RU consumption
func GetRequestCharge(iter *gocql.Iter) float64 {
	rawCharge, exists := iter.GetCustomPayload()["RequestCharge"]
	if !exists {
		return -1
	}
	return math.Float64frombits(binary.BigEndian.Uint64(rawCharge))
}

// Helpers for query time
type localObserver struct {
	queryDuration *time.Duration
}

func CreateLocalObserver() localObserver {
	return localObserver{queryDuration: new(time.Duration)}
}

func (t localObserver) ObserveQuery(ctx context.Context, oq gocql.ObservedQuery) {
	*t.queryDuration = time.Since(oq.Start)
}

func (t localObserver) Duration() time.Duration {
	return *t.queryDuration
}

func (t localObserver) Milliseconds() float64 {
	return float64(t.Duration()) / float64(time.Millisecond)
}