:auto MATCH (n)
CALL {
    WITH n
    DETACH DELETE n
} IN TRANSACTIONS;

CREATE CONSTRAINT IF NOT EXISTS FOR (o:Object) REQUIRE o.object_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (o:Blob) REQUIRE o.object_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (o:Tree) REQUIRE o.object_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (o:Commit) REQUIRE o.object_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (o:Tag) REQUIRE o.object_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (r:Reference) REQUIRE r.name IS UNIQUE;

:auto LOAD CSV WITH HEADERS FROM "file:///blob.csv" AS obj
CALL {
  WITH obj
  CREATE (o:Object:Blob{object_id: obj.object_id, content: apoc.text.base64Decode(coalesce(obj.content, ''))})
} IN TRANSACTIONS;

:auto LOAD CSV WITH HEADERS FROM "file:///tree.csv" AS obj
CALL {
  WITH obj
  CREATE (o:Object:Tree{object_id: obj.object_id})
} IN TRANSACTIONS;

:auto LOAD CSV WITH HEADERS FROM "file:///tag.csv" AS obj
CALL {
  WITH obj
  CREATE (o:Object:Tag{object_id: obj.object_id, content: apoc.text.base64Decode(coalesce(obj.content, ''))})
} IN TRANSACTIONS;

:auto LOAD CSV WITH HEADERS FROM "file:///commit.csv" AS obj
CALL {
  WITH obj
  MATCH (t:Tree{object_id:obj.tree})
  CREATE (o:Object:Commit{
  	object_id: obj.object_id, 
  	subject: apoc.text.base64Decode(coalesce(obj.subject, '')),
  	message: apoc.text.base64Decode(coalesce(obj.message, '')),
  	author_name: apoc.text.base64Decode(coalesce(obj.author_name, '')),
  	author_email: apoc.text.base64Decode(coalesce(obj.author_email, '')),
  	author_date: datetime({epochSeconds: toInteger(obj.author_date_epoch), timezone: obj.author_date_tz}),
  	committer_name: apoc.text.base64Decode(coalesce(obj.committer_name, '')),
  	committer_email: apoc.text.base64Decode(coalesce(obj.committer_email, '')),
  	committer_date: datetime({epochSeconds: toInteger(obj.committer_date_epoch), timezone: obj.committer_date_tz})
  })-[:TREE]->(t)
} IN TRANSACTIONS;

:auto LOAD CSV WITH HEADERS FROM "file:///commit_parents.csv" AS obj
CALL {
  WITH obj
  MATCH (c:Commit{object_id:obj.commit_oid}), (p:Commit{object_id:obj.parent_oid})
  CREATE (c)-[:PARENT{ordinal:obj.ordinal}]->(p)
} IN TRANSACTIONS;

:auto LOAD CSV WITH HEADERS FROM "file:///tree_entries.csv" AS obj
CALL {
  WITH obj
  MATCH (t:Tree{object_id:obj.tree_oid}), (o:Object{object_id:obj.entry_oid})
  CREATE (t)-[:ENTRY{path: apoc.text.base64Decode(obj.path), mode: obj.mode}]->(o)
} IN TRANSACTIONS;

:auto LOAD CSV WITH HEADERS FROM "file:///references.csv" AS ref 
CALL {
    WITH ref
    MATCH (o:Object{object_id:ref.object_id})
    CREATE (r:Reference{name:ref.reference})-[:REFERENCES]->(o)
} IN TRANSACTIONS;
