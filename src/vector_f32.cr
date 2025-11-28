struct FalkorDB::VectorF32
  getter values : Array(Float32)

  def self.matches_falkordb_type?(type : ::FalkorDB::ValueType) : Bool
    type.vector_f32?
  end

  def self.from_falkordb_value(type : FalkorDB::ValueType, value, cache)
    new value.as(Array).map(&.as(String).to_f32)
  end

  def initialize(@values)
  end

  def to_a
    values
  end
end
