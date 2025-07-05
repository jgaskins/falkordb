require "spec"
require "../src/falkordb"

macro define_test(redis)
  private macro test(msg, **options, &block)
    it(\{{msg}}, \{{options.double_splat}}) do
      graph = FalkorDB::Graph.new(redis, UUID.v4.to_s)

      begin
        \{{yield}}
      ensure
        graph.delete!
      end
    end
  end
end

module Spec::Expectations
  def be_within(delta, of expected)
    be_close(expected, delta)
  end
end
