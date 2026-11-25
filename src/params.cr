module FalkorDB
  macro params(type, *vars)
    struct {{type}} < ::FalkorDB::Params
      {% for var in vars %}
        getter {{var}}
      {% end %}

      def initialize(
        {% for var in vars %}
          {{var}},
        {% end %}
      )
      end

      {{yield}}
    end
  end

  abstract struct Params
  end
end
