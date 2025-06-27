# frozen_string_literal: true

require_relative "aggregation/version"
require "active_support/lazy_load_hooks"
require "active_record"

module ActiverecordBatch
  module Aggregation
    class Error < StandardError; end

    # Simple proxy that delegates count and where operations to preloaded data
    class AggregationProxy
      def initialize(loader, record, reflection, conditions = {})
        @loader = loader
        @record = record
        @reflection = reflection
        @conditions = conditions
      end

      def count(*)
        @loader.get_association_count(@record, @reflection, @conditions)
      end

      def where(conditions)
        new_conditions = @conditions.merge(conditions)
        self.class.new(@loader, @record, @reflection, new_conditions)
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

      def get_association_count(record, reflection, conditions)
        cache_key = [reflection.name, conditions.sort].inspect
        @lock.synchronize { load_association_count(reflection, conditions, cache_key) unless @loaded_data.key?(cache_key) }

        primary_key_value = record.public_send(@primary_key)
        @loaded_data.dig(cache_key, primary_key_value) || 0
      end

      private

      def load_association_count(reflection, conditions, cache_key)
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
          query = query.where(conditions) if conditions.present?
          @loaded_data[cache_key] = query.group("#{group_by_table}.#{group_by_key}").count
        else
          query = reflection.klass.where(reflection.foreign_key => subquery)
          query = query.where(conditions) if conditions.present?
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
