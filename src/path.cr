require "./node"
require "./relationship"

struct FalkorDB::Path
  getter nodes : Array(Node)
  getter relationships : Array(Relationship)

  def self.matches_falkordb_type?(type : ::FalkorDB::ValueType) : Bool
    type.path?
  end

  def self.from_falkordb_value(type : FalkorDB::ValueType, value, cache)
    value = value.as(Array)
    nodes_raw, relationships_raw = value.as(Array)
    nodes_type, nodes_value = nodes_raw.as(Array)
    rels_type, rels_value = relationships_raw.as(Array)
    nodes = ValueType.value_for(nodes_type, nodes_value, cache).as(Array).map(&.as(Node))
    rels = ValueType.value_for(rels_type, rels_value, cache).as(Array).map(&.as(Relationship))
    new(
      nodes: nodes,
      relationships: rels,
    )
  end

  def initialize(@nodes, @relationships)
  end
end
