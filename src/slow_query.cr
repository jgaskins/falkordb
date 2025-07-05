module FalkorDB
  record SlowQuery, timestamp : Time, command : String, query : String, duration : Time::Span do
    def self.new(array : Array)
      timestamp_string, command, query, duration_string = array
      new(
        timestamp: Time.unix_ms(timestamp_string.as(String).to_i64),
        command: command.as(String),
        query: query.as(String),
        duration: duration_string.as(String).to_f.milliseconds,
      )
    end

    def self.new(value : Redis::Value)
      raise ArgumentError.new("Expected an array of strings, got: #{value.inspect}")
    end
  end
end
