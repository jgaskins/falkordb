require "./point"
require "./node"
require "./relationship"

module FalkorDB
  alias Value = String |
                Nil |
                Bool |
                Int64 |
                Float64 |
                Node |
                Relationship |
                Path |
                Array(Value) |
                Hash(String, Value) |
                Point |
                VectorF32 |
                LocalDateTime |
                LocalDate |
                LocalTime |
                Duration
  alias List = Array(Value)
  alias Map = Hash(String, Value)
end
