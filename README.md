# FalkorDB

This shard is a Crystal client for [FalkorDB](https://www.falkordb.com), a graph database built on top of Redis.

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     falkordb:
       github: jgaskins/falkordb
   ```

2. Run `shards install`

## Usage

```crystal
require "falkordb"

redis = Redis::Client.new
graph = FalkorDB::Graph.new(redis, "your-graph-key")
```

Then, when you run your queries, use the `read_query` and `write_query` methods. If you're using a `Redis::ReplicationClient`, this will automatically route write queries to the primary and read queries to replicas.

```crystal
graph.write_query <<-CYPHER
  CREATE (user:User {
    id: randomUUID(),
    name: "Jamie"
  })
CYPHER
```

### Specifying query return type

You can set the return type of the results using the `return:` argument. This must match the types of the values in your Cypher query's `RETURN` clause.

```crystal
# We need to get the first result from the result set to get the id itself
id = graph.write_query(<<-CYPHER, return: UUID).first
  CREATE (user:User {
    id: randomUUID(),
    name: "Jamie"
  })
  RETURN user.id
CYPHER
```

### Returning multiple values per result

If your `RETURN` clause has multiple values, you can specify their types using tuples:

```crystal
graph.read_query(<<-CYPHER, return: {User, Team})
  MATCH (user:User)-[:MEMBER_OF]->(team:Team)
  RETURN user, team
CYPHER
```

### Passing query parameters

If you need to pass values to your query, it's best to use query parameters rather than interpolate them into the strings. This offers the same benefits as for SQL databases:

- Improved performance via caching query plans
- Avoiding query injection via malformed values

To pass query parameters, simply place them after your Cypher query. For example, to get all of the teams the current user is a member of, you might use a query that looks like this:

```crystal
graph.read_query <<-CYPHER, {user_id: current_user.id}, return: Team do |team|
  MATCH (user:User{id: $user_id})-[:MEMBER_OF]->(team:Team)
  RETURN team
CYPHER
```

## Contributing

1. Fork it (<https://github.com/jgaskins/falkordb/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Jamie Gaskins](https://github.com/jgaskins) - creator and maintainer
