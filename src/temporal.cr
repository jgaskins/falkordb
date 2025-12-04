module FalkorDB
  struct LocalDateTime
    @time : Time

    def self.matches_falkordb_type?(type : ::FalkorDB::ValueType) : Bool
      type.datetime?
    end

    def self.from_falkordb_value(type : FalkorDB::ValueType, value, cache)
      new Time.unix value.as(Int64)
    end

    def self.new(year : Int, month : Int, day : Int, hour : Int, minute : Int, second : Int)
      new Time.utc(
        year: year,
        month: month,
        day: day,
        hour: hour,
        minute: minute,
        second: second,
      )
    end

    def initialize(@time)
    end

    def to_time : Time
      @time
    end
  end

  struct LocalDate
    getter year : Int16
    getter month : Int8
    getter day : Int8

    def self.matches_falkordb_type?(type : ::FalkorDB::ValueType) : Bool
      type.date?
    end

    def self.from_falkordb_value(type : FalkorDB::ValueType, value, cache)
      date = Time.unix(value.as(Int64))

      new(
        year: date.year.to_i16,
        month: date.month.to_i8,
        day: date.day.to_i8,
      )
    end

    def initialize(@year, @month, @day)
    end
  end

  struct LocalTime
    getter hour : Int16
    getter minute : Int8
    getter second : Int8

    def self.matches_falkordb_type?(type : ::FalkorDB::ValueType) : Bool
      type.time?
    end

    def self.from_falkordb_value(type : FalkorDB::ValueType, value, cache)
      date = Time.unix(value.as(Int64))

      new(
        hour: date.hour.to_i16,
        minute: date.minute.to_i8,
        second: date.second.to_i8,
      )
    end

    def initialize(@hour, @minute, @second)
    end
  end

  struct Duration
    @span : Time::Span

    def self.matches_falkordb_type?(type : ::FalkorDB::ValueType) : Bool
      type.duration?
    end

    def self.from_falkordb_value(type : FalkorDB::ValueType, value, cache)
      span = value.as(Int64).seconds

      new span
    end

    def self.new(years, months, days, hours, minutes, seconds)
      new (years.days * 366) + (months.days * 30) + days.days + hours.hours + minutes.minutes + seconds.seconds
    end

    def initialize(@span)
    end

    def to_span
      @span
    end
  end
end
