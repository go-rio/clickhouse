package clickhouse_test

import (
	"context"
	"log"
	"time"

	"github.com/go-rio/clickhouse"
	"github.com/go-rio/rio"
)

// Event maps the table
//
//	CREATE TABLE events (id UInt64, kind String, at DateTime64(6, 'UTC'))
//	ENGINE = MergeTree ORDER BY id
//
// ClickHouse never backfills generated values, so the key is supplied.
type Event struct {
	ID   uint64 `rio:",pk,noautoincr"`
	Kind string
	At   time.Time
}

func ExampleOpen() {
	ctx := context.Background()
	db, err := clickhouse.Open(ctx, "clickhouse://default:secret@localhost:9000/analytics?max_open_conns=4")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := rio.Insert(ctx, db, &Event{ID: 1, Kind: "click", At: time.Now()}); err != nil {
		log.Fatal(err)
	}
	since := time.Now().Add(-time.Hour)
	events, err := rio.From[Event]().
		Where("kind = ? AND at >= ?", "click", since).
		OrderBy("at").
		All(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%d click events", len(events))
}

func ExampleOpenSQL() {
	sqlDB, err := clickhouse.OpenSQL("clickhouse://default:secret@localhost:9000/analytics")
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	var n uint64
	row := sqlDB.QueryRow("SELECT count() FROM events WHERE kind = ?", "click")
	if err := row.Scan(&n); err != nil {
		log.Fatal(err)
	}
	log.Printf("%d click events", n)
}
