# frozen_string_literal: true

require_relative "aggregation/version"
require "active_support/lazy_load_hooks"
require "active_record"

module ActiverecordBatch
  module Aggregation
    class Error < StandardError; end

    class AggregationProxy
      def initialize(loader, record, reflection)
        @loader = loader
        @record = record
        @reflection = reflection
        @associated_records = nil
      end

      def count(*_args, &)
        associated_records.count
      end

      def where(conditions)
        filtered_records = associated_records.select do |assoc_record|
          conditions.all? { |key, value| assoc_record.public_send(key) == value }
        end

        result = Object.new
        result.define_singleton_method(:count) do
          filtered_records.count
        end
        result
      end

      private

      def associated_records
        @associated_records ||= @loader.get_associated_records(@record, @reflection)
      end
    end

    class AggregationLoader
      def initialize(records)
        @records = records
        @loaded_data = {}
        @klass = records.first.class
        @primary_key = @klass.primary_key
        @lock = Mutex.new
      end

      def proxy_for(record, reflection)
        AggregationProxy.new(self, record, reflection)
      end

      def get_associated_records(record, reflection)
        @lock.synchronize do
          load_association(reflection) unless @loaded_data.key?(reflection.name)
        end

        pk_value = record.public_send(@primary_key)
        @loaded_data.dig(reflection.name, pk_value) || []
      end

      private

      def load_association(reflection)
        foreign_key = reflection.foreign_key
        assoc_klass = reflection.klass
        record_ids = @records.map(&@primary_key.to_sym)

        all_associated_records = assoc_klass.where(foreign_key => record_ids).to_a
        @loaded_data[reflection.name] = all_associated_records.group_by(&foreign_key.to_sym)
      end
    end

    module ModelMethods
      def eager
        all.eager
      end
    end

    module RelationMethods
      def eager
        relation = clone
        relation.instance_variable_set(:@perform_eager_aggregation, true)
        relation
      end
    end

    module RelationExecution
      private

      def exec_queries
        records = super

        if instance_variable_defined?(:@perform_eager_aggregation) && !records.empty?
          loader = AggregationLoader.new(records)

          records.each do |record|
            record.class.reflect_on_all_associations(:has_many).each do |reflection|
              next if reflection.options[:through]

              association_name = reflection.name

              record.define_singleton_method(association_name) do
                loader.proxy_for(self, reflection)
              end
            end
          end
        end

        records
      end
    end
  end
end

ActiveSupport.on_load(:active_record) do
  ActiveRecord::Base.extend(ActiverecordBatch::Aggregation::ModelMethods)
  ActiveRecord::Relation.include(ActiverecordBatch::Aggregation::RelationMethods)
  ActiveRecord::Relation.prepend(ActiverecordBatch::Aggregation::RelationExecution)
end
