#! /bin/sh
podman run --publish=7474:7474 --publish=7687:7687 --env=NEO4J_AUTH=none --env=NEO4J_db_import_csv_buffer__size=75000000 --volume=$(PWD):/import --rm --env=NEO4J_PLUGINS=\[\"apoc\"\] --env=NEO4J_server_memory_heap_max__size=4G neo4j
