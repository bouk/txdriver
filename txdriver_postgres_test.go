package txdriver

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"

	"github.com/lib/pq"
)

func createPostgresTestDB(t testing.TB) *TransactionDBCreator {
	testDatabase := fmt.Sprintf("test_db_%d", rand.Int63())
	ctx := context.Background()

	createConn, err := pq.NewConnector("user=postgres port=14125 sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	createDB := sql.OpenDB(createConn)
	statement := fmt.Sprintf(`CREATE DATABASE %s`, testDatabase)
	_, err = createDB.ExecContext(ctx, statement)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := pq.NewConnector("user=postgres port=14125 sslmode=disable dbname=" + testDatabase)
	if err != nil {
		t.Fatal(err)
	}

	insertDB := sql.OpenDB(conn)
	if _, err = insertDB.ExecContext(ctx, `CREATE TABLE users (id BIGSERIAL PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer createDB.Close()
		if _, err := createDB.ExecContext(ctx, fmt.Sprintf("DROP DATABASE %s WITH (FORCE)", testDatabase)); err != nil {
			t.Error(err)
		}
	})
	insertDB.Close()

	txdb, err := FromConnector(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	return txdb
}

func TestPostgres(t *testing.T) {
	ctx := context.TODO()
	txdb := createPostgresTestDB(t)
	for i := 0; i < 10; i++ {
		db, err := txdb.DB(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var id int
		err = db.QueryRowContext(ctx, "INSERT INTO users DEFAULT VALUES RETURNING id").Scan(&id)
		if err != nil {
			t.Fatal(err)
		}
		if id != 1 {
			t.Errorf("unexpected insert ID: %d!=1", id)
		}
		db.Close()
	}
}
