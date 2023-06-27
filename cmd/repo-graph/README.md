1. `cd` into the `repo-graph` directory.
1. Generate the *.csv files to import into the database by running `go run main.go <path-to-.git-dir>`
1. Run `./launch_db.sh` to launch the database. Web console is available at `localhost:7474`.
1. Run the query from `load_csv.cypher` in the web console to load the data.
