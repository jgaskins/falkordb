require "json"

module FalkorDB
  struct Point
    include ::JSON::Serializable

    getter latitude : Float64
    getter longitude : Float64

    def self.matches_falkordb_type?(type : ::FalkorDB::ValueType) : Bool
      type.array?
    end

    def self.from_falkordb_value(type : FalkorDB::ValueType, value, cache)
      raw_lat, raw_long = value.as(Array)
      lat = Float64.from_falkordb_value(:double, raw_lat, cache)
      long = Float64.from_falkordb_value(:double, raw_long, cache)
      new lat, long
    end

    def initialize(@latitude, @longitude)
    end
  end
end
