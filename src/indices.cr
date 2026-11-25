require "./error"

module FalkorDB
  record Indices, graph : Graph do
    def create(label : String, property : String)
      create label, {property}
    end

    def create(label : String, properties : Enumerable(String))
      cypher = String.build do |str|
        str << "CREATE INDEX FOR (n:" << label << ") ON ("
        properties.each_with_index 1 do |property, index|
          str << "n." << property
          if index < properties.size
            str << ", "
          end
        end

        str << ')'
      end

      begin
        graph.write_query cypher
      rescue ex : Redis::Error
        if (msg = ex.message) && (match = msg.match(/Attribute '(.*)' is already indexed/))
          raise IndexAlreadyExists.new("Index on #{label.inspect} already indexes #{match[1].inspect}")
        else
          raise ex
        end
      end
    end

    # Create a full-text index on the given properties of nodes with the given
    # label.
    #
    # ```
    # graph.indices.create_fulltext_node "User", "name", "bio"
    # ```
    def create_fulltext_node(label : String, *properties : String)
      cypher = String.build do |str|
        str << "CREATE FULLTEXT INDEX FOR (n:" << label << ") ON ("
        properties.join str, ", " do |property, io|
          io << "n." << property
        end
        str << ')'
      end
      graph.write_query cypher
    end

    # Create a full-text index with per-field options (`weight`, `nostem`,
    # `phonetic`) using the `db.idx.fulltext.createNodeIndex` procedure.
    #
    # NOTE: FalkorDB 6.0 does not support this procedure form with arguments
    # and its DDL syntax has no per-field options, so this overload only works
    # on FalkorDB 4.x.
    #
    # ```
    # graph.indices.create_fulltext_node "User", "id", {field: "name", nostem: true, weight: 2}
    # ```
    def create_fulltext_node(label, *properties)
      cypher = String.build do |str|
        str << "CALL db.idx.fulltext.createNodeIndex("
        label.to_json str
        # TODO: Extract the `encode_property` somewhere. It's the same way we
        # encode query parameters. It seems to be how FalkorDB encodes Map data
        # structures generally.
        properties.each do |property|
          str << ", "
          encode_property property, str
        end
        str << ')'
      end
      graph.write_query cypher
    end

    private def encode_property(property : String | Int::Primitive | Float::Primitive | Bool | Nil | Array, io : IO)
      property.inspect io
    end

    private def encode_property(property : NamedTuple, io : IO)
      io << '{'
      property.each_with_index 1 do |key, value, index|
        io << key << ": "
        encode_property value, io
        if index < property.size
          io << ", "
        end
      end
      io << '}'
    end

    def list
      return_types = {
        String,                           # label
        Array(String),                    # properties
        Hash(String, Array(Index::Type)), # types
        Hash(String, Map),                # options
        String,                           # language
        Array(String),                    # stopwords
        Index::EntityType,                # entitytype
        Index::Status,                    # status
        Map,                              # info
      }

      result = graph.read_query <<-CYPHER, return: return_types
        CALL db.indexes()
        YIELD label, properties, types, options, language, stopwords, entitytype, status, info
        RETURN label, properties, types, options, language, stopwords, entitytype, status, info
        CYPHER

      result.map do |row|
        Index.new(*row)
      end
    end

    class IndexAlreadyExists < Error
    end
  end

  record Index,
    label : String,
    properties : Array(String),
    types : Hash(String, Array(Index::Type)),
    options : Hash(String, Map),
    language : String,
    stopwords : Array(String),
    entity_type : EntityType,
    status : Status,
    info : Map do
    enum Type
      RANGE
      FULLTEXT
    end

    enum EntityType
      NODE
      RELATIONSHIP
    end

    enum Status
      # The index is still being populated after being created
      UNDER_CONSTRUCTION
      OPERATIONAL

      # :nodoc:
      def self.from_falkordb_value(type : FalkorDB::ValueType, value, cache) : self
        status = String.from_falkordb_value(type, value, cache)
        # While FalkorDB is populating an index, the status includes its
        # progress, such as "[Indexing] 0/500000: UNDER CONSTRUCTION".
        parse status.rpartition(": ").last.tr(" ", "_")
      end
    end
  end
end
