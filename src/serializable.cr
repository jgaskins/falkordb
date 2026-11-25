require "json"
require "uuid/json"
require "uri/json"

require "./error"
require "./point"
require "./temporal"

module FalkorDB
  annotation Field
  end

  annotation NodeLabel
  end

  annotation RelationshipType
  end

  # The `FalkorDB::Serializable::*` mixins tell `FalkorDB::Client` how
  # to deserialize nodes and relationships as your own Crystal object types,
  # similar to [`DB::Serializable`](http://crystal-lang.github.io/crystal-db/api/0.11.0/DB/Serializable.html).
  #
  # ```
  # require "redis/graph"
  #
  # struct Person
  #   include FalkorDB::Serializable::Node
  #
  #   getter id : UUID
  #   getter name : String
  #   getter created_at : Time
  # end
  #
  # struct Team
  #   include FalkorDB::Serializable::Node
  #
  #   getter name : String
  # end
  #
  # struct Membership
  #   include FalkorDB::Serializable::Relationship
  #
  #   getter since : Time
  # end
  #
  # redis = Redis::Client.new
  # redis.del "my-graph"
  #
  # # Store the graph data in the Redis key "my-graph"
  # graph = redis.graph(key: "my-graph")
  #
  # id = UUID.random
  #
  # # Create some data in our graph
  # pp graph.write_query <<-CYPHER, id: id, name: "Jamie", now: Time.utc.to_unix_ms, team_name: "My Team"
  #   CREATE (:Person{id: $id, name: $name, created_at: $now})-[:MEMBER_OF{since: $now}]->(team:Team{name: $team_name})
  # CYPHER
  #
  # # The `return` argument specifies the return types of the results in your
  # # Cypher query's `RETURN` clause
  # pp graph.read_query(<<-CYPHER, {id: id}, return: {Person, Membership, Team})
  #   MATCH (person:Person{id: $id})-[membership:MEMBER_OF]->(team:Team)
  #   RETURN person, membership, team
  # CYPHER
  # ```
  module Serializable
    module Node
      record Metadata, id : Int64, labels : Array(String)

      def self.from_falkordb_value(node_type : T.class, value_type : FalkorDB::ValueType, value, cache) : T forall T
        id, label_ids, properties = value.as(Array)

        id = id.as Int64
        labels = label_ids.as(Array).map do |label_id|
          cache.label(label_id.as(Int64))
        end
        metadata = Metadata.new(id: id, labels: labels)

        T.new_from_falkordb_node(metadata, properties.as(Array), cache)
      end

      macro included
        # :nodoc:
        def self.new_from_falkordb_node(metadata : ::FalkorDB::Serializable::Node::Metadata, properties : Array, cache)
          instance = allocate
          instance.initialize(falkordb_metadata: metadata, properties: properties, cache: cache)
          GC.add_finalizer(instance) if instance.responds_to?(:finalize)
          instance
        end

        def self.from_falkordb_value(type : FalkorDB::ValueType, value, cache)
          ::FalkorDB::Serializable::Node.from_falkordb_value(self, type, value, cache)
        end

        def self.matches_falkordb_type?(type : ::FalkorDB::ValueType) : Bool
          type.node?
        end

        def self.can_transform_graph_result?(value : Int64 | String | Nil | Redis::Error, cache) : Bool
          false
        end

        def self.can_transform_graph_result?(value : Array, cache) : Bool
          id, label_ids, properties = value.as(Array)
          labels = label_ids.as(Array).any? do |label_id|
            label = cache.label label_id.as(Int64)

            {% if (ann = @type.annotation(::FalkorDB::NodeLabel)) && ann[0] %}
              label == {{ann[0]}}
            {% else %}
              # Use the type name if there is no label specified for the type
              label == name
            {% end %}
          end
        end

        def self.can_transform_graph_result?(value : Redis::Value) : Bool
          false
        end
      end

      def initialize(
        *,
        falkordb_metadata metadata : ::FalkorDB::Serializable::Node::Metadata,
        properties : Array,
        cache,
      )
        {% begin %}
          {% for ivar in @type.instance_vars %}
            %found{ivar.name} = false
            %value{ivar.name} = uninitialized {{ivar.type}}
          {% end %}

          properties.as(Array).each do |property|
            property_id, type, value = property.as(Array)
            key = cache.property(property_id.as(Int64))
            case key
              {% for ivar in @type.instance_vars %}
                {% ann = ivar.annotation(::FalkorDB::Field) %}
                {% if ann.nil? || !ann[:ignore] %}
                  when "{{ivar.name}}"
                    %found{ivar.name} = true
                    %value{ivar.name} = 
                    {% if (ann = ivar.annotation(::FalkorDB::Field)) && ann[:converter] %}
                      {{ann[:converter]}}.from_falkordb_value(
                       {{ivar.type}}.from_falkordb_value(::FalkorDB::ValueType.new(type.as(Int).to_i), value, cache)
                      )
                    {% else %}
                      {{ivar.type}}.from_falkordb_value(::FalkorDB::ValueType.new(type.as(Int).to_i), value, cache)
                    {% end %}
                {% end %}
              {% end %}
            else
              unknown_falkordb_node_property key, value
            end
          end

          {% for ivar in @type.instance_vars %}
            {% ann = ivar.annotation(::FalkorDB::Field) %}
            {% if ann.nil? || !ann[:ignore] %}
              if %found{ivar.name}
                @{{ivar}} = %value{ivar.name}
              {% unless ivar.type.nilable? || ivar.has_default_value? %}
              else
                raise PropertyMissing.new("Node did not contain the property `{{ivar.name}}`")
              {% end %}
              end
            {% end %}
          {% end %}
        {% end %}
      end

      def unknown_falkordb_node_property(key, value)
      end

      class PropertyMissing < Error
      end
    end

    module Relationship
      record Metadata,
        id : Int64,
        type : String,
        source_node : Int64,
        destination_node : Int64

      def self.from_falkordb_value(node_type : T.class, value_type : FalkorDB::ValueType, value, cache) : T forall T
        id, type, source_node, destination_node, properties = value.as(Array)

        metadata = Metadata.new(
          id: id.as(Int64),
          type: cache.relationship_type(type.as(Int64)),
          source_node: source_node.as(Int64),
          destination_node: destination_node.as(Int64),
        )

        T.new_from_falkordb_relationship(metadata, properties.as(Array), cache)
      end

      macro included
        # :nodoc:
        def self.new_from_falkordb_relationship(metadata : ::FalkorDB::Serializable::Relationship::Metadata, properties : Array, cache)
          instance = allocate
          instance.initialize(metadata, properties, cache)
          GC.add_finalizer(instance) if instance.responds_to?(:finalize)
          instance
        end

        def self.from_falkordb_value(type : FalkorDB::ValueType, value, cache)
          ::FalkorDB::Serializable::Relationship.from_falkordb_value(self, type, value, cache)
        end

        def self.matches_falkordb_type?(type : ::FalkorDB::ValueType) : Bool
          type.relationship?
        end
      end

      def initialize(metadata : ::FalkorDB::Serializable::Relationship::Metadata, properties : Array, cache)
        {% begin %}
          {% for ivar in @type.instance_vars %}
            %found{ivar.name} = false
            %value{ivar.name} = uninitialized {{ivar.type}}
          {% end %}

          properties.as(Array).each do |property|
            property_id, type, value = property.as(Array)
            key = cache.property(property_id.as(Int64))
            case key
              {% for ivar in @type.instance_vars %}
                when "{{ivar.name}}"
                  %found{ivar.name} = true
                  %value{ivar.name} = 
                  {% if (ann = ivar.annotation(::FalkorDB::Field)) && ann[:converter] %}
                    {{ann[:converter]}}.from_falkordb_value(
                     {{ivar.type}}.from_falkordb_value(::FalkorDB::ValueType.new(type.as(Int).to_i), value, cache)
                    )
                  {% else %}
                    {{ivar.type}}.from_falkordb_value(::FalkorDB::ValueType.new(type.as(Int).to_i), value, cache)
                  {% end %}
              {% end %}
            else
              unknown_falkordb_node_property key, value
            end
          end

          {% for ivar in @type.instance_vars %}
            if %found{ivar.name}
              @{{ivar}} = %value{ivar.name}
            {% unless ivar.type.nilable? || ivar.has_default_value? %}
            else
              raise PropertyMissing.new("Relationship did not contain the property `{{ivar.name}}`")
            {% end %}
            end
          {% end %}
        {% end %}
      end

      def unknown_falkordb_node_property(key, value)
      end

      class PropertyMissing < Error
      end
    end

    module Property
      macro included
        include JSON::Serializable

        def self.from_falkordb_value(type : FalkorDB::ValueType, value, cache) : self
          new type, value, cache
        end
      end

      def initialize(type : FalkorDB::ValueType, value, cache)
        unless type.map?
          raise InvalidType.new("Expected {{@type}}, received #{value.inspect}")
        end

        {% for ivar in @type.instance_vars %}
          %type{ivar.name}, %value{ivar.name} = 
          @{{ivar.name}} = {{ivar.type}}.from_falkordb_value
        {% end %}
      end

      class InvalidType < Exception
      end
    end
  end
end
