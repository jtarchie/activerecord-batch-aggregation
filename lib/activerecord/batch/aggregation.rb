# frozen_string_literal: true

require_relative "aggregation/version"
require "active_support/lazy_load_hooks"
require "active_record"

module ActiverecordBatch
  module Aggregation
    class Error < StandardError; end

    # Simple proxy that lazily builds a relation and delegates count to the loader
    class AggregationProxy
      def initialize(loader, record, reflection, chain = [])
        @loader = loader
        @record = record
        @reflection = reflection
        @chain = chain
      end

      def count(*)
        @loader.get_association_count(@record, @reflection, @chain)
      end

      def where(*args)
        new_chain = @chain + [{ method: :where, args: args, block: nil }]
        self.class.new(@loader, @record, @reflection, new_chain)
      end

      def respond_to_missing?(method_name, include_private = false)
        # Check if the base association class responds to the method.
        @reflection.klass.all.respond_to?(method_name) || super
      end

      def method_missing(method_name, *args, &block)
        # Assume any missing method is a scope or relation method and chain it.
        new_chain = @chain + [{ method: method_name, args: args, block: block }]
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

      def proxy_for(record, reflection) = AggregationProxy.new(self, record, reflection)

      def get_association_count(record, reflection, chain)
        # Create a cache key from the reflection and the chain of methods.
        # Procs/blocks in the chain can't be reliably hashed, so we ignore them for the key.
        key_chain = chain.map { |c| [c[:method], c[:args]] }
        cache_key = [reflection.name, key_chain].inspect

        @lock.synchronize do
          unless @loaded_data.key?(cache_key)
            # If cache miss, build the relation from the chain and load the counts for all records.
            # This ensures dynamic scopes (like `1.month.ago`) are evaluated only once.
            relation = build_relation(reflection, chain)
            load_association_count(reflection, relation, cache_key)
          end
        end

        primary_key_value = record.public_send(@primary_key)
        @loaded_data.dig(cache_key, primary_key_value) || 0
      end

      private

      def build_relation(reflection, chain)
        # Start with the base relation for the association's class.
        relation = reflection.klass.all

        # Apply the association's own scope if it exists.
        relation = relation.instance_exec(&reflection.scope) if reflection.scope

        # Apply all the chained methods (scopes, where clauses, etc.).
        chain.each do |item|
          relation = relation.public_send(item[:method], *item[:args], &item[:block])
        end

        relation
      end

      def load_association_count(reflection, relation, cache_key)
        subquery = @relation.select(@primary_key)
        if reflection.options[:through]
          through_reflection = reflection.through_reflection

          join_reflection = reflection.klass.reflect_on_all_associations.find do |assoc|
            assoc.klass == through_reflection.klass
          end

          raise "Could not find association from #{reflection.klass.name} to #{through_reflection.klass.name}" unless join_reflection

          group_by_table = through_reflection.table_name
          group_by_key = through_reflection.foreign_key

          query = reflection.klass.joins(join_reflection.name)
          query = query.where(group_by_table => { group_by_key => subquery })
          query = query.merge(relation)
          @loaded_data[cache_key] = query.group("#{group_by_table}.#{group_by_key}").count
        else
          query = reflection.klass.where(reflection.foreign_key => subquery)
          query = query.merge(relation)
          @loaded_data[cache_key] = query.group(reflection.foreign_key).count
        end
      end
    end

    module ModelMethods
      def eager = all.eager
    end

    module RelationMethods
      def eager
        clone.tap { |relation| relation.instance_variable_set(:@perform_eager_aggregation, true) }
      end
    end

    module RelationExecution
      private

      def exec_queries
        records = super
        setup_eager_aggregation(self, records) if should_eager_aggregate?(records)
        records
      end

      def should_eager_aggregate?(records)
        instance_variable_defined?(:@perform_eager_aggregation) && !records.empty?
      end

      def setup_eager_aggregation(relation, records)
        loader = AggregationLoader.new(relation, records)

        records.each do |record|
          has_many_associations(record).each do |reflection|
            record.define_singleton_method(reflection.name) { loader.proxy_for(self, reflection) }
          end
        end
      end

      def has_many_associations(record)
        record.class.reflect_on_all_associations(:has_many)
      end
    end
  end
end

ActiveSupport.on_load(:active_record) do
  ActiveRecord::Base.extend(ActiverecordBatch::Aggregation::ModelMethods)
  ActiveRecord::Relation.include(ActiverecordBatch::Aggregation::RelationMethods)
  ActiveRecord::Relation.prepend(ActiverecordBatch::Aggregation::RelationExecution)
end
