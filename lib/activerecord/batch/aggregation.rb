# frozen_string_literal: true

require_relative "aggregation/version"
require "active_support/lazy_load_hooks"
require "active_record"

module ActiverecordBatch
  module Aggregation
    class Error < StandardError; end

    module EagerAggregation
      def count(column_name = nil)
        unscope(:select).calculate(:count, column_name)
      end
    end

    module RelationMethods
      def eager
        select(Arel.sql("#{table_name}.*, 1 AS count")).extending(EagerAggregation)
      end
    end
  end
end

ActiveSupport.on_load(:active_record) do
  ActiveRecord::Relation.include(ActiverecordBatch::Aggregation::RelationMethods)
end
