module FalkorDB
  record SlowQuery, timestamp : Time, command : String, query : String, duration : Time::Span, params : String? do
    def self.new(array : Array)
      timestamp_string, command, query, duration_string = array
      # Entries only include params if the query was given any
      params = array[4]?

      new(
        timestamp: Time.unix(timestamp_string.as(String).to_i64),
        command: command.as(String),
        query: query.as(String),
        duration: duration_string.as(String).to_f.milliseconds,
        params: params.as(String?).try(&.lchop("CYPHER ")),
      )
    end

    def self.new(value : Redis::Value)
      raise ArgumentError.new("Expected an array of strings, got: #{value.inspect}")
    end
  end
end
