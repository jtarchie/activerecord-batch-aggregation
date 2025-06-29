# frozen_string_literal: true

require_relative "aggregation/version"
require "active_support/lazy_load_hooks"
require "active_record"
require "active_record/connection_adapters/abstract/query_cache"

module ActiveRecord
  module Eager
    module Aggregation
      class Error < StandardError; end

      module Cache
        def self.cache
          ActiveSupport::IsolatedExecutionState[:active_record_eager_aggregation_cache] ||= {}
        end

        def self.clear_cache
          cache.clear
        end
      end

      # A promise-like object for deferring aggregation loading.
      class AsyncAggregationPromise
        def initialize(loader, function, record, reflection, chain, column)
          @loader = loader
          @function = function
          @record = record
          @reflection = reflection
          @chain = chain
          @column = column
          @loaded = false
        end

        def value
          return @value if defined?(@value)

          @value = @loader.get_association_aggregation(@function, @record, @reflection, @chain, @column)
        end
      end

      # Proxy for lazily building a relation and delegating count to the loader
      class AggregationProxy
        AGGREGATION_FUNCTIONS = %i[count average maximum minimum].freeze
        ASYNC_AGGREGATION_FUNCTIONS = [:async_sum] + AGGREGATION_FUNCTIONS.map { |f| :"async_#{f}" }.freeze
        ALL_AGGREGATION_FUNCTIONS = (AGGREGATION_FUNCTIONS + ASYNC_AGGREGATION_FUNCTIONS + %i[sum exists?]).freeze

        def initialize(loader, record, reflection, chain = [])
          @loader = loader
          @record = record
          @reflection = reflection
          @chain = chain
        end

        def map(&)
          relation.map(&)
        end

        def to_a
          relation.to_a
        end

        def where(*args, &block)
          chain_with(:where, args, block)
        end

        def exists?
          @loader.get_association_aggregation(:exists, @record, @reflection, @chain, "*")
        end

        def sum(initial_value_or_column = 0, &)
          return relation.sum(initial_value_or_column, &) if block_given?

          @loader.get_association_aggregation(:sum, @record, @reflection, @chain, initial_value_or_column)
        end

        def respond_to_missing?(method_name, include_private = false)
          ALL_AGGREGATION_FUNCTIONS.include?(method_name) || @reflection.klass.all.respond_to?(method_name, include_private) || super
        end

        def method_missing(method_name, *args, &block)
          if AGGREGATION_FUNCTIONS.include?(method_name)
            column = args.first || "*"
            return @loader.get_association_aggregation(method_name, @record, @reflection, @chain, column)
          end

          if ASYNC_AGGREGATION_FUNCTIONS.include?(method_name)
            sync_method = method_name.to_s.delete_prefix("async_").to_sym
            column = args.first || "*"
            return AsyncAggregationPromise.new(@loader, sync_method, @record, @reflection, @chain, column)
          end

          chain_with(method_name, args, block)
        end

        private

        def relation
          base_relation = @record.association(@reflection.name).scope
          @chain.inject(base_relation) do |rel, item|
            rel.public_send(item[:method], *item[:args], &item[:block])
          end
        end

        def chain_with(method, args, block)
          new_chain = @chain + [{ method: method, args: args, block: block }]
          self.class.new(@loader, @record, @reflection, new_chain)
        end
      end

      # Handles lazy loading of associations with thread safety
      class AggregationLoader
        def initialize(records, cache)
          @records = records
          @primary_key = records.first.class.primary_key
          @model_class = records.first.class
          @subquery_pks = records.map { |record| record.public_send(@primary_key) }
          @cache = cache
        end

        def self.new_from_pks(pks, model_class, cache)
          loader = allocate
          loader.instance_variable_set(:@subquery_pks, pks)
          loader.instance_variable_set(:@primary_key, model_class.primary_key)
          loader.instance_variable_set(:@model_class, model_class)
          loader.instance_variable_set(:@cache, cache)
          loader
        end

        def proxy_for(record, reflection)
          AggregationProxy.new(self, record, reflection)
        end

        def get_association_aggregation(function, record, reflection, chain, column)
          aggregation_results = @cache.fetch([function, reflection, chain, column]) do
            relation = build_relation(reflection, chain)
            @cache[[function, reflection, chain, column]] = load_association_aggregation(function, reflection, relation, column)
          end

          primary_key_value = record.public_send(@primary_key)

          return aggregation_results.include?(primary_key_value) if function == :exists

          result = aggregation_results[primary_key_value]

          if result.nil?
            return 0 if %i[count sum].include?(function)

            return
          end

          result
        end

        private

        def build_relation(reflection, chain)
          relation = reflection.klass.all
          relation = relation.merge(reflection.scope) if reflection.scope

          chain.inject(relation) do |rel, item|
            rel.public_send(item[:method], *item[:args], &item[:block])
          end
        end

        def load_association_aggregation(function, reflection, relation, column)
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
                              .where(group_by_table => { group_by_key => @subquery_pks })
                              .merge(relation)

            return query.distinct.pluck("#{group_by_table}.#{group_by_key}") if function == :exists

            if function == :count
              query = query.distinct
              column = reflection.klass.primary_key if column == "*"
            end

            query.group("#{group_by_table}.#{group_by_key}").public_send(function, column)
          else
            query = reflection.klass.joins(reflection.inverse_of.name)
                              .where(reflection.foreign_key => @subquery_pks)
                              .merge(relation)

            return query.distinct.pluck(reflection.foreign_key) if function == :exists

            query.group(reflection.foreign_key).public_send(function, column)
          end
        end
      end

      module ModelMethods
        def eager_aggregations
          all.eager_aggregations
        end
      end

      module RelationMethods
        def eager_aggregations
          clone.tap { |relation| relation.instance_variable_set(:@perform_eager_aggregation, true) }
        end
      end

      module BatchMethods
        def find_each(start: nil, finish: nil, batch_size: 1000, error_on_ignore: nil, order: :asc, **kwargs, &block)
          return super unless instance_variable_defined?(:@perform_eager_aggregation)
          return to_enum(:find_each, start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore, order: order, **kwargs) unless block_given?

          perform_in_batches(start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore, order: order, **kwargs) do |batch|
            batch.each(&block)
          end
        end

        def find_in_batches(start: nil, finish: nil, batch_size: 1000, error_on_ignore: nil, order: :asc, **, &)
          return super unless instance_variable_defined?(:@perform_eager_aggregation)
          return to_enum(:find_in_batches, start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore, order: order, **) unless block_given?

          perform_in_batches(start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore, order: order, **, &)
        end

        private

        def perform_in_batches(**options, &block)
          relation = clone
          relation.remove_instance_variable(:@perform_eager_aggregation)

          all_pks = relation.map(&relation.primary_key.to_sym)
          loader = AggregationLoader.new_from_pks(all_pks, relation.klass, Cache.cache)

          relation.find_in_batches(**options) do |batch|
            setup_eager_aggregation(batch, loader)
            block.call(batch)
          end
        end
      end

      module RelationExecution
        private

        def exec_queries
          records = super
          setup_eager_aggregation(records) if eager_aggregation_needed?(records)
          records
        end

        def eager_aggregation_needed?(records)
          instance_variable_defined?(:@perform_eager_aggregation) && records.present? && records.first.class.connection.query_cache_enabled
        end

        def setup_eager_aggregation(records, loader = nil)
          loader ||= AggregationLoader.new(records, Cache.cache)
          model_class = records.first.class
          has_many_reflections = model_class.reflect_on_all_associations(:has_many)

          records.each do |record|
            has_many_reflections.each do |reflection|
              record.define_singleton_method(reflection.name) do
                loader.proxy_for(self, reflection)
              end
            end
          end
        end
      end

      module QueryCacheExtension
        def clear_query_cache
          Cache.clear_cache
          super
        end
      end
    end
  end
end

ActiveSupport.on_load(:active_record) do
  ActiveRecord::Base.extend(ActiveRecord::Eager::Aggregation::ModelMethods)
  ActiveRecord::Relation.include(ActiveRecord::Eager::Aggregation::RelationMethods)
  ActiveRecord::Relation.prepend(ActiveRecord::Eager::Aggregation::BatchMethods)
  ActiveRecord::Relation.prepend(ActiveRecord::Eager::Aggregation::RelationExecution)
  ActiveRecord::ConnectionAdapters::AbstractAdapter.prepend(ActiveRecord::Eager::Aggregation::QueryCacheExtension)
end
