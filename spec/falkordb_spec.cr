require "./spec_helper"

redis = Redis::Client.new(URI.parse("redis://localhost:6380"))
define_test redis

describe FalkorDB do
  test "runs basic queries" do
    # We need to write to the graph to ensure the graph's Redis key exists
    graph.write_query "CREATE (n)"
    result = graph.read_query("RETURN 42 AS value")

    result.fields.should eq %w[value]
    result.first.first.should eq 42
  end

  test "can return any of the FalkorDB types" do
    graph.write_query("return null").first.should eq [nil]
    graph.write_query("return 'asdf'").first.should eq ["asdf"]
    graph.write_query("return 123").first.should eq [123]
    graph.write_query("return true, false").first.should eq [true, false]
    graph.write_query("return 123.456").first.should eq [123.456]
    graph.write_query("return [1, 'a', true]").first.should eq [[1, "a", true]]
    graph.write_query("create (:A{id: 1})-[rel:REL{id: 2}]->(:B{id: 3}) RETURN rel").first.should eq([
      FalkorDB::Relationship.new(
        id: 0,
        type: "REL",
        src_node: 0,
        dest_node: 1,
        properties: FalkorDB::Map{
          "id" => 2i64,
        },
      ),
    ])
    graph.write_query("create (n:MyNode{id: 123}) RETURN n").first.should eq([
      FalkorDB::Node.new(
        id: 2,
        labels: %w[MyNode],
        properties: FalkorDB::Map{
          "id" => 123i64,
        },
      ),
    ])
    # Reusing the path we created above for the relationship check
    graph.write_query("match p=()-[:REL]->() RETURN p").first.should eq([
      FalkorDB::Path.new(
        nodes: [
          FalkorDB::Node.new(
            id: 0,
            labels: %w[A],
            properties: FalkorDB::Map{
              "id" => 1i64,
            },
          ),
          FalkorDB::Node.new(
            id: 1,
            labels: %w[B],
            properties: FalkorDB::Map{
              "id" => 3i64,
            },
          ),
        ],
        relationships: [
          FalkorDB::Relationship.new(
            id: 0,
            type: "REL",
            src_node: 0,
            dest_node: 1,
            properties: FalkorDB::Map{
              "id" => 2i64,
            },
          ),
        ],
      ),
    ])
    graph.write_query("return {id: 123}").first.should eq([FalkorDB::Map{"id" => 123i64}])
    graph.write_query("return point({latitude: 1, longitude: 2})").first.should eq([
      FalkorDB::Point.new(
        latitude: 1,
        longitude: 2,
      ),
    ])
    graph.write_query("return vecf32([1.2, 3.4])").first.should eq [FalkorDB::VectorF32.new([1.2, 3.4] of Float32)]
    graph.write_query("return localdatetime({year: 2025, month: 11, day: 28, hour: 2, minute: 4, second: 25, nanosecond: 123456789})").first.should eq [FalkorDB::LocalDateTime.new(
      year: 2025,
      month: 11,
      day: 28,
      hour: 2,
      minute: 4,
      second: 25,
    )]
    graph.write_query("return date({year: 2025, month: 11, day: 28})").first.should eq [
      FalkorDB::LocalDate.new(
        year: 2025,
        month: 11,
        day: 28,
      ),
    ]
    graph.write_query("return localtime({hour: 2, minute: 21, second: 9})").first.should eq [
      FalkorDB::LocalTime.new(
        hour: 2,
        minute: 21,
        second: 9,
      ),
    ]
    graph.write_query("return duration({years: 2, months: 6, days: 13, hours: 2, minutes: 21, seconds: 9})").first.should eq [
      FalkorDB::Duration.new(
        years: 2,
        months: 6,
        days: 13,
        hours: 2,
        minutes: 21,
        seconds: 9,
      ),
    ]
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
