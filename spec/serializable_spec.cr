require "./spec_helper"

private struct User
  include FalkorDB::Serializable::Node

  getter id : UUID
  getter name : String
  getter login_count : Int64 = 0
  getter created_at : Time
end

private struct Membership
  include FalkorDB::Serializable::Relationship

  getter since : Time = Time::UNIX_EPOCH
end

redis = Redis::Client.new(URI.parse("redis://localhost:6380"))
key = UUID.v7.to_s
graph = FalkorDB::Graph.new(redis, key)
cache = FalkorDB::Cache.new(redis, key)

describe FalkorDB::Serializable::Node do
  before_all do
    key = UUID.v7.to_s
    graph = FalkorDB::Graph.new(redis, key)
    cache = FalkorDB::Cache.new(redis, key)

    # Prime the label and property cache
    graph.run_write <<-CYPHER
      CREATE (user:User{
        id: randomUUID(),
        name: "Jamie",
        login_count: 123,
        created_at: 0
      })
    CYPHER
  end
  after_all { graph.delete! }

  it "parses Node results" do
    node_data = [
      0_i64,   # node id
      [0_i64], # labels
      [        # properties
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
      [        # properties
        [0_i64, 2_i64, "61704db7-1b34-44c0-a827-6f5def2de1a0"],
        [1_i64, 2_i64, "Jamie"],
        # no login_count
        [3_i64, 3_i64, 1795556980000_i64],
      ],
    ]

    user = FalkorDB::Serializable::Node.from_falkordb_value(User, :node, node_data, cache)

    user.should be_a User
    user.name.should eq "Jamie"
    user.login_count.should eq 0 # default value for User#login_count
    user.created_at.should eq Time.unix_ms(1795556980000)
  end
end

describe FalkorDB::Serializable::Relationship do
  before_all do
    key = UUID.v7.to_s
    graph = FalkorDB::Graph.new(redis, key)
    cache = FalkorDB::Cache.new(redis, key)

    graph.run_write <<-CYPHER
      CREATE ()-[membership:MEMBER_OF{since: 1795556980000}]->()
      RETURN membership
    CYPHER
  end

  it "parses Relationship results" do
    rel_data = [
      0_i64, # id
      0_i64, # relationship type
      0_i64, # source node
      1_i64, # destination node
      [      # properties
        [0_i64, 3_i64, 1795556980000_i64],
      ],
    ]

    membership = FalkorDB::Serializable::Relationship.from_falkordb_value(Membership, :edge, rel_data, cache)

    membership.should be_a Membership
    membership.since.should eq Time.unix_ms(1795556980000)
  end

  it "allows default values" do
    rel_data = [
      0_i64,              # id
      0_i64,              # relationship type
      0_i64,              # source node
      1_i64,              # destination node
      [] of Redis::Value, # empty properties to use default values
    ]

    membership = FalkorDB::Serializable::Relationship.from_falkordb_value(Membership, :edge, rel_data, cache)

    membership.should be_a Membership
    membership.since.should eq Time::UNIX_EPOCH
  end
end
