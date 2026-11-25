require "./spec_helper"

redis = Redis::Client.new(URI.parse("redis://localhost:6380"))
define_test redis

private macro test_can_serialize(value, query = "RETURN $value")
  %value = {{value}}
  test "can serialize #{%value.class}" do
    graph.write_query({{query}}, {value: %value}, return: %value.class)
      .first
      .should eq %value
  end
end

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

  describe "param serialization" do
    test_can_serialize 1i8
    test_can_serialize 1i16
    test_can_serialize 1i32
    test_can_serialize 1i64
    test_can_serialize 1.0f32
    test_can_serialize 1.0f64
    test_can_serialize nil
    test_can_serialize true
    test_can_serialize false
    test_can_serialize "test"
    # We serialize down to the millisecond, so we have to truncate the timestamp
    # here to the same precision for comparison
    test_can_serialize Time.unix_ms(Time.utc.to_unix_ms)
    # `return: FalkorDB::List` means "the whole row, as a list of columns", so
    # the macro's single-column comparison doesn't apply.
    test "can serialize FalkorDB::List" do
      value = FalkorDB::List{1i64, "two", true}
      graph.write_query("RETURN $value", {value: value}, return: FalkorDB::List)
        .first
        .should eq [value]
    end
    test_can_serialize [1,2,3]
    test_can_serialize FalkorDB::Map{"a" => 1i64, "b" => 2i64}
    test_can_serialize FalkorDB::Point.new(12, 34), "RETURN point($value)"
    test_can_serialize FalkorDB::LocalTime.new(12, 34, 56),
      "RETURN localtime($value)"
    test_can_serialize FalkorDB::LocalDate.new(2026, 2, 15),
      "RETURN date($value)"
    test_can_serialize FalkorDB::LocalDateTime.new(Time.unix(Time.utc.to_unix)),
      "RETURN localdatetime($value)"
    test_can_serialize FalkorDB::Duration.new(12.hours + 34.minutes + 56.seconds),
      "RETURN duration($value)"
    test "can serialize negative durations" do
      value = FalkorDB::Duration.new(-(1.hour + 2.minutes + 3.seconds))
      graph.write_query("RETURN duration($value)", {value: value}, return: FalkorDB::Duration)
        .first
        .should eq value
    end
    test "truncates fractional seconds in durations, because FalkorDB does" do
      graph.write_query("RETURN duration($value)", {value: FalkorDB::Duration.new(1.5.seconds)}, return: FalkorDB::Duration)
        .first
        .should eq FalkorDB::Duration.new(1.second)
    end
    test "can serialize strings containing escapes and characters special to Crystal" do
      value = %(quote:" backslash:\\ \n\t\r\b\f esc:\e nul:\u0000 del:\u007F interp:\#{x} 'single' é ✓ 😀)
      graph.write_query("RETURN $value", {value: value}, return: String)
        .first
        .should eq value
    end
    test_can_serialize FalkorDB::VectorF32.new([1f32, 2f32, 3f32]),
      "RETURN vecf32($value)"
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

    read.size.should eq 1
    read.first.should eq "Jamie"
  end

  test "writes and reads nodes" do
    written = graph.write_query("create (n:User{id: randomUUID()}) RETURN n", return: FalkorDB::Node).first
    read = graph.read_query("match (n:User) return n", return: FalkorDB::Node).first?

    written.should eq read
  end

  test "writes and reads relationships" do
    now = Time.utc.to_unix_ms
    written = graph.write_query "create (:User)-[membership:MEMBER_OF{since: $now}]->(:Team) return membership",
      {now: now},
      return: FalkorDB::Relationship
    written.first.type.should eq "MEMBER_OF"
    written.first.properties["since"].should eq now

    read = graph.read_query "match (:User)-[membership:MEMBER_OF]->(:Team) return membership",
      return: FalkorDB::Relationship
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

    # FalkorDB uses RediSearch for indexes, which creates them async, so we need
    # to handle the case where an index is still being built when we ask for it.
    test "lists indices that are under construction" do
      graph.write_query "UNWIND range(1, 50000) AS id CREATE (:User{id: id})"
      graph.indices.create label: "User", property: "id"

      graph.indices.list.first.status.under_construction?.should eq true
    end

    test "creates fulltext indices" do
      graph.indices.create_fulltext_node "User", "id", "name"

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

    test "lists constraints that are under construction" do
      # Constraints are created and checked async, so we need to create enough
      # nodes for the constraint to still be under construction when we check
      # but nott enough for me to get impatient when running the specs
      graph.write_query "UNWIND range(1, 50000) AS id CREATE (:User{id: id})"
      graph.indices.create "User", "id"
      graph.constraints.create :unique, node_label: "User", properties: %w[id]

      graph.constraints.list.first.status.under_construction?.should eq true
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

    # FalkorDB keeps a Filter above the index scan when the value is a param
    query_plan.should eq [
      "Project",
      "    Filter",
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

    query_plan[0].as(String).should start_with "Project | Records produced: 1, Execution time:"
    query_plan[1].as(String).should start_with "    Filter | Records produced: 1, Execution time:"
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
      new_graph.delete! if new_graph
    end
  end

  test "gets the log of the slowest queries" do
    # Gotta run a query slow enough to get it into the slow query log (10ms by
    # default) but not so long that I get annoyed when running the specs
    query = "UNWIND range(1, $count) AS index CREATE (:User{id: randomUUID(), name: $prefix + index})"
    graph.write_query query, count: 50_000, prefix: "User "

    slowlog = graph.slowlog

    slowlog.size.should eq 1
    slowlog.first.command.should eq "GRAPH.QUERY"
    slowlog.first.query.should eq query
    slowlog.first.params.should eq %(count=50000 prefix="User ")
    slowlog.first.duration.should be >= 10.milliseconds
    slowlog.first.timestamp.should be_within 1.minute, of: Time.utc
  end

  describe "user-defined functions (UDFs)" do
    test "creates UDF libraries" do
      graph.write_query "create (n)" # avoid error when deleting after the test
      graph.udf.load "test_utils", <<-UDF, replace: true
        function identity(x) { return x; }
        falkor.register("identity", identity);
      UDF

      graph
        .write_query("RETURN test_utils.identity(42)", return: Int64)
        .first
        .should eq 42
    ensure
      graph.udf.delete "test_utils" rescue nil
    end

    test "lists UDF libraries" do
      graph.write_query "create (n)" # avoid error when deleting after the test
      graph.udf.load "test_utils", <<-UDF, replace: true
        function identity(x) { return x; }
        falkor.register("identity", identity);
      UDF

      # For some reason the `contain` matcher doesn't work here.
      graph.udf.list.as(Array).includes?([
        "library_name", "test_utils",
        "functions", ["identity"],
      ]).should eq true
      graph.udf.list("test_utils").should eq [
        [
          "library_name", "test_utils",
          "functions", ["identity"],
        ],
      ]
    ensure
      graph.udf.delete "test_utils" rescue nil
    end
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
