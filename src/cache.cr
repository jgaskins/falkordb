require "redis/commands"

module FalkorDB
  class Cache
    getter redis : Redis::Commands
    getter key : String
    @label_mutex = Mutex.new
    @relationship_mutex = Mutex.new
    @property_mutex = Mutex.new

    def initialize(@redis, @key)
      @labels = [] of String
      @relationship_types = [] of String
      @properties = [] of String
    end

    def label(label_id : Int64) : String
      @label_mutex.synchronize do
        @labels.fetch label_id do
          fetch_new("labels", "label", @labels.size) do |row|
            @labels << row.as(Array)[0].as(Array)[1].as(String)
          end

          @labels[label_id]
        end
      end
    end

    def labels
      @label_mutex.synchronize do
        if @labels.empty?
          fetch_new("labels", "label", @labels.size) do |row|
            @labels << row.as(Array)[0].as(Array)[1].as(String)
          end
        end

        @labels
      end
    end

    def relationship_type(type_id : Int) : String
      @relationship_mutex.synchronize do
        @relationship_types.fetch type_id do
          refresh_relationships

          @relationship_types[type_id]
        end
      end
    end

    def relationship_types
      @relationship_mutex.synchronize do
        refresh_relationships if @relationship_types.empty?

        @relationship_types
      end
    end

    def properties
      @property_mutex.synchronize do
        refresh_properties if @properties.empty?

        @properties
      end
    end

    def property(property_id : Int64) : String
      @property_mutex.synchronize do
        @properties.fetch property_id do
          refresh_properties

          @properties[property_id]
        end
      end
    end

    private def refresh_relationships : Nil
      fetch_new("relationshipTypes", "relationshipType", @relationship_types.size) do |row|
        @relationship_types << row.as(Array)[0].as(Array)[1].as(String)
      end
    end

    def refresh_properties : Nil
      fetch_new("propertyKeys", "propertyKey", @properties.size) do |row|
        @properties << row.as(Array)[0].as(Array)[1].as(String)
      end
    end

    def clear
      initialize @redis, @key
    end

    private def fetch_new(function, column, current_size, &)
      cypher = <<-CYPHER
        CALL db.#{function}() YIELD #{column}
        RETURN #{column}
        SKIP $current_size
        CYPHER
      query = Graph.new(@redis, @key, self).build_query(cypher, {current_size: current_size})
      response = @redis.run({"GRAPH.QUERY", @key, query, "--compact"}).as(Array)

      response[1].as(Array).each { |row| yield row }
    end
  end
end
