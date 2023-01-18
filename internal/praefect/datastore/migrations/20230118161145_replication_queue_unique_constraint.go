package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20230118161145_replication_queue_unique_constraint",
		Up: []string{`
			--- Delete existing duplicates from the replication_queue table
			DELETE FROM replication_queue a
			USING replication_queue b
			WHERE
				a.id > b.id
				AND a.job = b.job
				AND a.state = b.state;
`, `
			--- Create the unique index which will prevent duplicates
			CREATE UNIQUE INDEX replication_queue_constraint on replication_queue (
				job,
				state
			);
		`},
		Down: []string{"DROP INDEX replication_queue_constraint;"},
	}

	allMigrations = append(allMigrations, m)
}
