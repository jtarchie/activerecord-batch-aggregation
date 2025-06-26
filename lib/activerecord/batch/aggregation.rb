# frozen_string_literal: true

require_relative "aggregation/version"
require "active_support/lazy_load_hooks"
require "active_record"

module ActiverecordBatch
  module Aggregation
    class Error < StandardError; end

    # Simple proxy that delegates count and where operations to preloaded data
    class AggregationProxy
      def initialize(loader, record, reflection)
        @loader = loader
        @record = record
        @reflection = reflection
      end

      def count(*) = associated_records.count

      def where(conditions)
        filtered = associated_records.select do |record|
          conditions.all? { |key, value| record.public_send(key) == value }
        end

        Object.new.tap { |obj| obj.define_singleton_method(:count) { filtered.count } }
      end

      private

      def associated_records = @loader.get_associated_records(@record, @reflection)
    end

    # Handles lazy loading of associations with thread safety
    class AggregationLoader
      def initialize(records)
        @records = records
        @loaded_data = {}
        @primary_key = records.first.class.primary_key
        @lock = Mutex.new
      end

      def proxy_for(record, reflection) = AggregationProxy.new(self, record, reflection)

      def get_associated_records(record, reflection)
        @lock.synchronize { load_association(reflection) unless @loaded_data.key?(reflection.name) }

        primary_key_value = record.public_send(@primary_key)
        @loaded_data.dig(reflection.name, primary_key_value) || []
      end

      private

      def load_association(reflection)
        record_ids = @records.map(&@primary_key.to_sym)
        associated_records = reflection.klass.where(reflection.foreign_key => record_ids).to_a
        @loaded_data[reflection.name] = associated_records.group_by(&reflection.foreign_key.to_sym)
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
        setup_eager_aggregation(records) if should_eager_aggregate?(records)
        records
      end

      def should_eager_aggregate?(records)
        instance_variable_defined?(:@perform_eager_aggregation) && !records.empty?
      end

      def setup_eager_aggregation(records)
        loader = AggregationLoader.new(records)

        records.each do |record|
          has_many_associations(record).each do |reflection|
            record.define_singleton_method(reflection.name) { loader.proxy_for(self, reflection) }
          end
        end
      end

      def has_many_associations(record)
        record.class.reflect_on_all_associations(:has_many).reject { |r| r.options[:through] }
      end
    end
  end
end

ActiveSupport.on_load(:active_record) do
  ActiveRecord::Base.extend(ActiverecordBatch::Aggregation::ModelMethods)
  ActiveRecord::Relation.include(ActiverecordBatch::Aggregation::RelationMethods)
  ActiveRecord::Relation.prepend(ActiverecordBatch::Aggregation::RelationExecution)
end
