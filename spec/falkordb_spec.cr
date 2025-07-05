require "./spec_helper"

redis = Redis::Client.new(URI.parse("redis://localhost:6380"))
define_test redis

describe FalkorDB do
  test "runs basic queries" do
    result = graph.read_query("RETURN 42 AS value")

    result.fields.should eq %w[value]
    result.first.first.should eq 42
  end

  test "sets the return type" do
    result = graph.write_query <<-CYPHER, return: UUID
      CREATE (user:User {
        id: randomUUID(),
        name: "Jamie"
      })
      RETURN user.id
    CYPHER

    typeof(result.first).should eq UUID
  end

  test "returns multiple types" do
    result = graph.write_query <<-CYPHER, return: {UUID, String}
      CREATE (user:User {
        id: randomUUID(),
        name: "Jamie"
      })
      RETURN user.id, user.name
    CYPHER

    typeof(result.first).should eq(Tuple(UUID, String))
  end

  test "passes query parameters" do
    graph.write_query "create (u:User{id: 1, name: $name})", name: "Jamie"
    read = graph.read_query <<-CYPHER, {id: 1}, return: String
      match (u:User{id: $id})
      return u.name
    CYPHER

    read.first.should eq "Jamie"
  end

  test "writes and reads nodes" do
    written = graph.write_query("create (n:User{id: randomUUID()}) RETURN n", return: FalkorDB::Node).first
    read = graph.read_query("match (n:User) return n", return: FalkorDB::Node).first?

    written.should eq read
  end

  test "writes and reads relationships" do
    now = Time.utc.to_unix_ms
    written = graph.write_query("create (:User)-[membership:MEMBER_OF{since: $now}]->(:Team) return membership", {now: now}, return: FalkorDB::Relationship)
    written.first.type.should eq "MEMBER_OF"
    written.first.properties["since"].should eq now

    read = graph.read_query("match (:User)-[membership:MEMBER_OF]->(:Team) return membership", return: FalkorDB::Relationship)
    read.first.should eq written.first
  end

  test "deserializes custom node types" do
    now = Time.utc.to_unix_ms
    params = {
      user: {
        id:         UUID.v7,
        name:       "Jamie",
        created_at: now,
      },
      membership: {
        since: now,
      },
      team: {
        id:   UUID.v7,
        name: "Crystal Developers",
      },
    }
    written = graph.write_query(<<-CYPHER, params, return: {User, TeamMembership, Team})
      CREATE (u:User)
      SET u = $user
      CREATE (t:Team)
      SET t = $team

      CREATE (u)-[m:MEMBER_OF]->(t)
      SET m = $membership
      RETURN u, m, t
    CYPHER

    typeof(written.first).should eq Tuple(User, TeamMembership, Team)
    user, membership, team = written.first
    user.name.should eq "Jamie"
    user.created_at.should eq Time.unix_ms(now)
    membership.since.should eq Time.unix_ms(now)
    team.name.should eq "Crystal Developers"
  end

  describe "indexing" do
    test "creates range indices" do
      graph.indices.create label: "User", property: "id"

      index = graph.indices.list.first

      index.label.should eq "User"
      index.properties.should eq %w[id]
    end

    test "creates fulltext indices" do
      graph.indices.create_fulltext_node "User",
        "id",
        {
          field:  "name",
          nostem: true,
          weight: 2,
        }

      index = graph.indices.list.first

      index.label.should eq "User"
      index.properties.should eq %w[id name]
      index.types["id"].first.fulltext?.should eq true
      index.types["name"].first.fulltext?.should eq true

      graph.write_query <<-CYPHER, now: Time.utc.to_unix_ms
        CREATE (:User{
          id: randomUUID(),
          name: "Included in results",
          created_at: $now
        })
        CREATE (:User{
          id: randomUUID(),
          name: "Excluded from results",
          created_at: $now
        })
      CYPHER

      result = graph.read_query <<-CYPHER, return: User
        CALL db.idx.fulltext.queryNodes('User', 'included')
        YIELD node AS user, score
        RETURN user
        ORDER BY score DESC
      CYPHER

      result.rows.size.should eq 1
      result.first.name.should eq "Included in results"
    end
  end

  describe "constraints" do
    test "creates and lists unique constraints" do
      # Unique constraints must have a matching index
      graph.indices.create "User", "id"

      graph.constraints.create :unique, node_label: "User", properties: %w[id]
      constraint = graph.constraints.list.first

      constraint.entity_type.node?.should eq true
      constraint.type.unique?.should eq true
      constraint.type.mandatory?.should eq false
      constraint.label.should eq "User"
      constraint.properties.should eq %w[id]
    end

    test "creates and lists mandatory constraints" do
      graph.constraints.create :mandatory, node_label: "User", properties: %w[id]
      constraint = graph.constraints.list.first

      constraint.entity_type.node?.should eq true
      constraint.type.unique?.should eq false
      constraint.type.mandatory?.should eq true
      constraint.label.should eq "User"
      constraint.properties.should eq %w[id]
    end

    test "drops constraints" do
      graph.constraints.create :mandatory, node_label: "User", properties: %w[id]
      graph.constraints.drop :mandatory, node_label: "User", properties: %w[id]

      graph.constraints.list.should be_empty
    end
  end

  test "explains queries" do
    graph.indices.create "User", "id"
    user = graph.write_query(<<-CYPHER, {now: Time.utc.to_unix_ms}, return: User).first
      CREATE (user:User{id: randomUUID(), name: "Jamie", created_at: $now})
      RETURN user
    CYPHER

    query_plan = graph.explain <<-CYPHER, {id: user.id}
      MATCH (user:User{id: $id})
      RETURN user
    CYPHER

    query_plan.should eq [
      "Results",
      "    Project",
      "        Node By Index Scan | (user:User)",
    ]
  end

  test "profiles queries" do
    graph.indices.create "User", "id"
    user = graph.write_query(<<-CYPHER, {now: Time.utc.to_unix_ms}, return: User).first
      CREATE (user:User{id: randomUUID(), name: "Jamie", created_at: $now})
      RETURN user
    CYPHER

    query_plan = graph.profile <<-CYPHER, {id: user.id}
      MATCH (user:User{id: $id})
      RETURN user
    CYPHER

    query_plan[0].as(String).should start_with "Results | Records produced: 1, Execution time:"
    query_plan[1].as(String).should start_with "    Project | Records produced: 1, Execution time:"
    query_plan[2].as(String).should start_with "        Node By Index Scan | (user:User) | Records produced: 1, Execution time:"
  end

  # This the `GRAPH.MEMORY` command is documented, but doesn't seem to be working?
  pending "gets memory stats" do
    graph.write_query <<-CYPHER
      CREATE (:User{id: randomUUID(), name: "Jamie", created_at: 1751666023845})
    CYPHER
    pp graph.memory_usage
  end

  test "lists graphs" do
    # Gotta write to the graph for it to be created
    graph.write_query "CREATE (:Team{id: randomUUID(), name: 'FalkorDB Users'})"

    graph.list.should contain graph.key
  end

  test "copies a graph to a new key" do
    new_key = UUID.v7.to_s
    graph.write_query "CREATE (:Team{id: randomUUID(), name: 'FalkorDB Users'})"

    begin
      new_graph = graph.copy new_key
      graph.list.should contain new_key
      new_graph.key.should_not eq graph.key
      new_graph.key.should eq new_key

      results = new_graph.read_query <<-CYPHER, return: Team
        MATCH (t:Team)
        RETURN t
      CYPHER

      results.size.should eq 1
      results.first.name.should eq "FalkorDB Users"
    ensure
      redis.unlink new_key
    end
  end

  test "gets the log of the slowest queries" do
    graph.write_query "CREATE (:Team{id: randomUUID(), name: 'FalkorDB Users'})"

    slowlog = graph.slowlog

    slowlog.size.should eq 1
    slowlog.first.command.should eq "GRAPH.QUERY"
    slowlog.first.query.should eq "CREATE (:Team{id: randomUUID(), name: 'FalkorDB Users'})"
  end
end

struct User
  include FalkorDB::Serializable::Node

  getter id : UUID
  getter name : String
  getter created_at : Time
end

struct TeamMembership
  include FalkorDB::Serializable::Relationship

  getter since : Time
end

struct Team
  include FalkorDB::Serializable::Node

  getter id : UUID
  getter name : String
end
