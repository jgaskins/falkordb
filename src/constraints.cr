require "./cache"

record FalkorDB::Constraints, graph : Graph, redis : Redis::Commands, key : String, cache : Cache do
  def create(constraint_type : Type, *, node_label : String, properties : Array(String))
    operation :create, constraint_type, :node, node_label, properties
  end

  def drop(constraint_type : Type, *, node_label : String, properties : Array(String))
    operation :drop, constraint_type, :node, node_label, properties
  end

  def list
    list_results graph.write_query(<<-CYPHER, return: {String, String, Array(String), String, String})
      CALL db.constraints() YIELD type, label, properties, entitytype, status
      RETURN type, label, properties, entitytype, status
      CYPHER
  end

  def list(*, node label : String)
    list_results graph.write_query(<<-CYPHER, {label: label}, return: {String, String, Array(String), String, String})
      CALL db.constraints() YIELD type, label, properties, entitytype, status
      WHERE label = $label
      AND entitytype = 'NODE'
      RETURN type, label, properties, entitytype, status
      CYPHER
  end

  def list(*, relationship type : String)
    list_results graph.write_query(<<-CYPHER, {label: label}, return: {String, String, Array(String), String, String})
      CALL db.constraints() YIELD type, label, properties, entitytype, status
      WHERE label = $label
      AND entitytype = 'RELATIONSHIP'
      RETURN type, label, properties, entitytype, status
      CYPHER
  end

  private def operation(operation : Operation, constraint_type : Type, entity_type : EntityType, entity_name : String, properties : Array(String))
    # GRAPH.CONSTRAINT CREATE g UNIQUE NODE Person PROPERTIES 2 first_name last_name
    command = Array(String).new(initial_capacity: 8 + properties.size)
    command << "GRAPH.CONSTRAINT" << operation.to_s << @key
    command << constraint_type.to_s << entity_type.to_s << entity_name
    command << "PROPERTIES" << properties.size.to_s
    command.concat properties
    result = redis.run(command)
  end

  private def list_results(results)
    # https://docs.falkordb.com/commands/graph.constraint-create.html#listing-constraints
    # type	type of constraint, either UNIQUE or MANDATORY
    # label	label or relationship-type enforced by the constraint
    # properties	list of properties enforced by the constraint
    # entitytype	type of entity, either NODE or RELATIONSHIP
    # status	either UNDER CONSTRUCTION, OPERATIONAL or FAILED
    results.map do |(type, label, properties, entity_type, status)|
      Constraint.new(
        type: Type.parse(type),
        label: label,
        properties: properties,
        entity_type: EntityType.parse(entity_type),
        status: Status.parse(status),
      )
    end
  end

  enum Type
    UNIQUE
    MANDATORY
  end

  enum EntityType
    NODE
    RELATIONSHIP
  end

  enum Status
    UNDER_CONSTRUCTION
    OPERATIONAL
    FAILED
  end

  private enum Operation
    CREATE
    DROP
  end

  record Constraint,
    type : Type,
    label : String,
    properties : Array(String),
    entity_type : EntityType,
    status : Status
end
