require "./spec_helper"

private def param(value) : String
  String.build { |io| value.to_falkordb_param io }
end

# FalkorDB's compact result format encodes each value as a `[type_id, value]`
# pair, where `type_id` is a `FalkorDB::ValueType`.
private def result(type : FalkorDB::ValueType, value) : Array(Redis::Value)
  [type.value.to_i64, value] of Redis::Value
end

describe "#to_falkordb_param" do
  it "serializes strings as quoted Cypher string literals" do
    param("hello").should eq %("hello")
  end

  it "escapes quotes, backslashes, and newlines in strings" do
    param(%(a "quoted" \\ string\n)).should eq %("a \\"quoted\\" \\\\ string\\n")
  end

  it "uses Cypher's named escapes for whitespace control characters" do
    param("\t\r\b\f").should eq %("\\t\\r\\b\\f")
  end

  it "escapes other control characters as \\u sequences" do
    param("\e\u0000\u007F").should eq %("\\u001B\\u0000\\u007F")
  end

  it "does not escape characters that are only special in Crystal" do
    param("\#{x} 'single' é ✓ 😀").should eq %("\#{x} 'single' é ✓ 😀")
  end

  it "serializes integers of every size" do
    param(1i8).should eq "1"
    param(-2i16).should eq "-2"
    param(3i32).should eq "3"
    param(Int64::MAX).should eq Int64::MAX.to_s
  end

  it "serializes floats" do
    param(1.5f32).should eq "1.5"
    param(-2.25f64).should eq "-2.25"
  end

  it "serializes booleans" do
    param(true).should eq "true"
    param(false).should eq "false"
  end

  it "serializes nil as null" do
    param(nil).should eq "null"
  end

  it "serializes a Time as Unix milliseconds" do
    param(Time.unix_ms(1_700_000_000_123)).should eq "1700000000123"
  end

  it "serializes a UUID as a string" do
    uuid = UUID.new("01234567-89ab-cdef-0123-456789abcdef")
    param(uuid).should eq %("01234567-89ab-cdef-0123-456789abcdef")
  end

  it "serializes arrays, recursively serializing their elements" do
    param([1, 2, 3]).should eq "[1, 2, 3]"
    param(["a", nil, [true]] of String | Nil | Array(Bool)).should eq %(["a", null, [true]])
    param([] of Int32).should eq "[]"
  end

  it "serializes hashes as Cypher maps" do
    param({"a" => 1, "b" => "two"}).should eq %({a:1, b:"two"})
    param({} of String => Int32).should eq "{}"
  end

  it "serializes named tuples as Cypher maps" do
    param({a: 1, b: [nil]}).should eq "{a:1, b:[null]}"
  end

  it "serializes a Point as a latitude/longitude map" do
    param(FalkorDB::Point.new(12.5, -34.25)).should eq "{latitude: 12.5,longitude: -34.25}"
  end

  it "serializes a VectorF32 as a list of floats" do
    param(FalkorDB::VectorF32.new([1f32, 2.5f32])).should eq "[1.0, 2.5]"
  end

  it "serializes a LocalDate as a zero-padded ISO-8601 date string" do
    param(FalkorDB::LocalDate.new(2026, 2, 5)).should eq "'2026-02-05'"
  end

  it "serializes a LocalTime as a zero-padded ISO-8601 time string" do
    param(FalkorDB::LocalTime.new(1, 2, 3)).should eq "'01:02:03'"
  end

  it "serializes a LocalDateTime as a zero-padded ISO-8601 datetime string" do
    param(FalkorDB::LocalDateTime.new(2026, 2, 5, 1, 2, 3)).should eq "'2026-02-05T01:02:03'"
  end

  describe FalkorDB::Duration do
    it "serializes as an ISO-8601 duration in seconds" do
      param(FalkorDB::Duration.new(12.hours + 34.minutes + 56.seconds)).should eq "'PT45296S'"
    end

    it "serializes durations longer than a day" do
      param(FalkorDB::Duration.new(26.hours)).should eq "'PT93600S'"
    end

    it "serializes a zero duration" do
      param(FalkorDB::Duration.new(0.seconds)).should eq "'PT0S'"
    end

    it "serializes fractional seconds" do
      param(FalkorDB::Duration.new(1.5.seconds)).should eq "'PT1.5S'"
      param(FalkorDB::Duration.new(1.nanosecond)).should eq "'PT0.000000001S'"
    end

    it "serializes negative durations with the sign inside the PT" do
      param(FalkorDB::Duration.new(-(1.hour + 2.minutes + 3.seconds))).should eq "'PT-3723S'"
      param(FalkorDB::Duration.new(-1.5.seconds)).should eq "'PT-1.5S'"
      param(FalkorDB::Duration.new(-500.milliseconds)).should eq "'PT-0.5S'"
    end
  end

  it "refuses to serialize graph entities" do
    node = FalkorDB::Node.new(1, ["User"], FalkorDB::Map.new)
    relationship = FalkorDB::Relationship.new(1, "MEMBER_OF", 1, 2, FalkorDB::Map.new)
    path = FalkorDB::Path.new([node], [relationship])

    expect_raises FalkorDB::Error, "Cannot serialize a FalkorDB::Node as a param" do
      param(node)
    end
    expect_raises FalkorDB::Error, "Cannot serialize a FalkorDB::Relationship as a param" do
      param(relationship)
    end
    expect_raises FalkorDB::Error, "Cannot serialize a FalkorDB::Path as a param" do
      param(path)
    end
  end
end

describe ".from_falkordb_result" do
  {% for type in %w[Float32 Float64] %}
    describe {{type.id}} do
      it "parses a double" do
        {{type.id}}.from_falkordb_result(result(:double, "1.5")).should eq 1.5
      end

      it "rejects non-double values" do
        expect_raises ArgumentError do
          {{type.id}}.from_falkordb_result(result(:string, "1.5"))
        end
      end
    end
  {% end %}

  describe Nil do
    it "parses null" do
      Nil.from_falkordb_result(result(:null, nil)).should eq nil
    end

    it "rejects non-null values" do
      expect_raises ArgumentError do
        Nil.from_falkordb_result(result(:double, "1.5"))
      end
    end
  end

  describe Array do
    it "parses an array, deserializing each element" do
      value = [result(:double, "1.5"), result(:double, "2.5")] of Redis::Value
      Array(Float64).from_falkordb_result(result(:array, value)).should eq [1.5, 2.5]
    end

    it "parses nested arrays" do
      inner = [result(:double, "1.5")] of Redis::Value
      value = [result(:array, inner)] of Redis::Value
      Array(Array(Float64)).from_falkordb_result(result(:array, value)).should eq [[1.5]]
    end

    it "rejects non-array values" do
      expect_raises ArgumentError do
        Array(Float64).from_falkordb_result(result(:double, "1.5"))
      end
    end

    it "rejects values that aren't FalkorDB results" do
      expect_raises ArgumentError do
        Array(Float64).from_falkordb_result("nope")
      end
    end
  end

  describe Union do
    it "parses whichever member type matches the result" do
      (Float64 | Nil).from_falkordb_result(result(:double, "1.5")).should eq 1.5
      (Float64 | Nil).from_falkordb_result(result(:null, nil)).should eq nil
    end

    it "rejects results that match none of its member types" do
      expect_raises ArgumentError do
        (Float64 | Nil).from_falkordb_result(result(:string, "nope"))
      end
    end
  end
end

private enum ConstraintLikeStatus
  UNDER_CONSTRUCTION
  OPERATIONAL
end

describe "Enum.from_falkordb_value" do
  it "parses multi-word values that FalkorDB separates with spaces" do
    ConstraintLikeStatus.from_falkordb_value(:string, "UNDER CONSTRUCTION", nil)
      .should eq ConstraintLikeStatus::UNDER_CONSTRUCTION
  end

  it "parses single-word values" do
    ConstraintLikeStatus.from_falkordb_value(:string, "OPERATIONAL", nil)
      .should eq ConstraintLikeStatus::OPERATIONAL
  end
end

describe FalkorDB::Index::Status do
  it "parses the status of an index that is still being populated" do
    FalkorDB::Index::Status.from_falkordb_value(:string, "[Indexing] 0/500000: UNDER CONSTRUCTION", nil)
      .should eq FalkorDB::Index::Status::UNDER_CONSTRUCTION
  end

  it "parses the status of a populated index" do
    FalkorDB::Index::Status.from_falkordb_value(:string, "OPERATIONAL", nil)
      .should eq FalkorDB::Index::Status::OPERATIONAL
  end
end
