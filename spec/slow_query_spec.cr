require "./spec_helper"

describe FalkorDB::SlowQuery do
  it "parses a slowlog entry for a query with params" do
    query = FalkorDB::SlowQuery.new([
      "1795547387",
      "GRAPH.QUERY",
      "UNWIND range(1, $count) AS i RETURN i",
      "28.919",
      "CYPHER count=20000",
    ] of Redis::Value)

    query.timestamp.should eq Time.unix(1795547387)
    query.command.should eq "GRAPH.QUERY"
    query.query.should eq "UNWIND range(1, $count) AS i RETURN i"
    query.duration.should eq 28.919.milliseconds
    query.params.should eq "count=20000"
  end

  it "removes only the CYPHER prefix from params" do
    # Param names starting with letters in "CYPHER " must survive intact
    query = FalkorDB::SlowQuery.new([
      "1795547387", "GRAPH.QUERY", "RETURN $Rate, $Count", "28.919",
      "CYPHER Rate=1 Count=2",
    ] of Redis::Value)

    query.params.should eq "Rate=1 Count=2"
  end

  it "parses a slowlog entry for a query without params" do
    # FalkorDB omits the params field entirely in this case
    query = FalkorDB::SlowQuery.new([
      "1795547387", "GRAPH.QUERY", "RETURN 1", "28.919",
    ] of Redis::Value)

    query.query.should eq "RETURN 1"
    query.params.should be_nil
  end
end
