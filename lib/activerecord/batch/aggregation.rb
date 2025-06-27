# frozen_string_literal: true

require_relative "aggregation/version"
require "active_support/lazy_load_hooks"
require "active_record"

module ActiverecordBatch
  module Aggregation
    class Error < StandardError; end

    # Proxy for lazily building a relation and delegating count to the loader
    class AggregationProxy
      AGGREGATION_FUNCTIONS = %i[count sum average maximum minimum].freeze

      def initialize(loader, record, reflection, chain = [])
        @loader = loader
        @record = record
        @reflection = reflection
        @chain = chain
      end

      def where(*args, &block)
        chain_with(:where, args, block)
      end

      def respond_to_missing?(method_name, include_private = false)
        AGGREGATION_FUNCTIONS.include?(method_name) || @reflection.klass.all.respond_to?(method_name, include_private) || super
      end

      def method_missing(method_name, *args, &block)
        if AGGREGATION_FUNCTIONS.include?(method_name)
          column = args.first || "*"
          return @loader.get_association_aggregation(method_name, @record, @reflection, @chain, column)
        end

        chain_with(method_name, args, block)
      end

      private

      def chain_with(method, args, block)
        new_chain = @chain + [{ method: method, args: args, block: block }]
        self.class.new(@loader, @record, @reflection, new_chain)
      end
    end

    # Handles lazy loading of associations with thread safety
    class AggregationLoader
      def initialize(relation, records)
        @relation = relation
        @records = records
        @loaded_data = {}
        @primary_key = records.first.class.primary_key
        @lock = Mutex.new
      end

      def proxy_for(record, reflection)
        AggregationProxy.new(self, record, reflection)
      end

      def get_association_aggregation(function, record, reflection, chain, column)
        cache_key = build_cache_key(reflection, chain, function, column)

        @lock.synchronize do
          @loaded_data[cache_key] ||= begin
            relation = build_relation(reflection, chain)
            load_association_aggregation(function, reflection, relation, column)
          end
        end

        primary_key_value = record.public_send(@primary_key)
        result = @loaded_data[cache_key][primary_key_value]

        if result.nil?
          return 0 if %i[count sum].include?(function)

          return
        end

        result
      end

      private

      def build_cache_key(reflection, chain, function, column)
        key_chain = chain.map { |c| [c[:method], c[:args]] }
        [reflection.name, key_chain, function, column].inspect
      end

      def build_relation(reflection, chain)
        relation = reflection.klass.all
        relation = relation.instance_exec(&reflection.scope) if reflection.scope

        chain.inject(relation) do |rel, item|
          rel.public_send(item[:method], *item[:args], &item[:block])
        end
      end

      def load_association_aggregation(function, reflection, relation, column)
        subquery = @relation.select(@primary_key)
        if reflection.options[:through]
          through_reflection = reflection.through_reflection
          join_reflection = reflection.klass.reflect_on_all_associations.find do |assoc|
            assoc.klass == through_reflection.klass
          end

          raise "Could not find association from #{reflection.klass.name} to #{through_reflection.klass.name}" unless join_reflection

          group_by_table = through_reflection.table_name
          group_by_key = through_reflection.foreign_key

          joins_arg = { join_reflection.name => through_reflection.inverse_of.name }
          query = reflection.klass.joins(joins_arg)
                            .where(group_by_table => { group_by_key => subquery })
                            .merge(relation)

          query.group("#{group_by_table}.#{group_by_key}").public_send(function, column)
        else
          query = reflection.klass.joins(reflection.inverse_of.name)
                            .where(reflection.foreign_key => subquery)
                            .merge(relation)

          query.group(reflection.foreign_key).public_send(function, column)
        end
      end
    end

    module ModelMethods
      def with_aggregations
        all.with_aggregations
      end
    end

    module RelationMethods
      def with_aggregations
        clone.tap { |relation| relation.instance_variable_set(:@perform_eager_aggregation, true) }
      end
    end

    module RelationExecution
      private

      def exec_queries
        records = super
        setup_eager_aggregation(self, records) if eager_aggregation_needed?(records)
        records
      end

      def eager_aggregation_needed?(records)
        instance_variable_defined?(:@perform_eager_aggregation) && records.present?
      end

      def setup_eager_aggregation(relation, records)
        loader = AggregationLoader.new(relation, records)
        has_many_associations = ->(record) { record.class.reflect_on_all_associations(:has_many) }

        records.each do |record|
          has_many_associations.call(record).each do |reflection|
            record.define_singleton_method(reflection.name) { loader.proxy_for(self, reflection) }
          end
        end
      end
    end
  end
end

ActiveSupport.on_load(:active_record) do
  ActiveRecord::Base.extend(ActiverecordBatch::Aggregation::ModelMethods)
  ActiveRecord::Relation.include(ActiverecordBatch::Aggregation::RelationMethods)
  ActiveRecord::Relation.prepend(ActiverecordBatch::Aggregation::RelationExecution)
end
