require "./spec_helper"

private struct User
  include FalkorDB::Serializable::Node

  getter id : UUID
  getter name : String
  getter login_count : Int64 = 0
  getter created_at : Time
end

redis = Redis::Client.new(URI.parse("redis://localhost:6380"))
key = UUID.v7.to_s
graph = FalkorDB::Graph.new(redis, key)
cache = FalkorDB::Cache.new(redis, key)

describe FalkorDB::Serializable::Node do
  before_all do
    # Prime the label and property cache
    graph.run_write <<-CYPHER
      CREATE (user:User{
        id: randomUUID(),
        name: "Jamie",
        login_count: 123,
        created_at: 0
      })
      RETURN user
    CYPHER
  end
  after_all { graph.delete! }

  it "parses Node results" do
    node_data = [
      0_i64,   # node id
      [0_i64], # labels
      [       # properties
        [0_i64, 2_i64, "61704db7-1b34-44c0-a827-6f5def2de1a0"],
        [1_i64, 2_i64, "Jamie"],
        [2_i64, 3_i64, 123_i64],
        [3_i64, 3_i64, 1795556980000_i64],
      ],
    ]
    user = FalkorDB::Serializable::Node.from_falkordb_value(User, :node, node_data, cache)

    user.name.should eq "Jamie"
    user.login_count.should eq 123
    user.created_at.should eq Time.unix_ms(1795556980000)
  end

  it "allows default values" do
    node_data = [
      0_i64,   # node id
      [0_i64], # labels
      [       # properties
        [0_i64, 2_i64, "61704db7-1b34-44c0-a827-6f5def2de1a0"],
        [1_i64, 2_i64, "Jamie"],
        # no login_count
        [3_i64, 3_i64, 1795556980000_i64],
      ],
    ]
    user = FalkorDB::Serializable::Node.from_falkordb_value(User, :node, node_data, cache)

    user.name.should eq "Jamie"
    user.login_count.should eq 0
    user.created_at.should eq Time.unix_ms(1795556980000)
  end
end
