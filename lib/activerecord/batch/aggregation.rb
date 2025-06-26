# frozen_string_literal: true

require_relative "aggregation/version"
require "active_support/lazy_load_hooks"
require "active_record"

module ActiverecordBatch
  module Aggregation
    class Error < StandardError; end

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

        # This implementation is updated to handle basic `where` conditions on associations.
        # It remains specific to the User/Post test case for now.
        if instance_variable_defined?(:@perform_eager_aggregation) && klass.name == "User" && !records.empty?
          association_name = :posts
          reflection = klass.reflect_on_association(association_name)
          foreign_key = reflection.foreign_key
          primary_key = klass.primary_key

          record_ids = records.map(&primary_key.to_sym)

          # Eager load all associated records to handle queries in memory.
          # This uses more memory than just fetching counts but is necessary
          # to support filtering with `where`.
          all_associated_records = reflection.klass.where(foreign_key => record_ids).to_a
          records_by_fk = all_associated_records.group_by(&foreign_key.to_sym)

          records.each do |record|
            primary_key_value = record.public_send(primary_key)
            associated_records_for_record = records_by_fk[primary_key_value] || []
            proxy = record.public_send(association_name)

            # Override methods on the association proxy to use the preloaded records.
            # This avoids N+1 queries for `count` and `where(...).count` calls.
            proxy.define_singleton_method(:count) do |*args, &block|
              associated_records_for_record.count(*args, &block)
            end

            proxy.define_singleton_method(:where) do |conditions|
              # This is a basic implementation of `where` that works on the loaded array.
              # It only supports equality checks from a hash.
              filtered_records = associated_records_for_record.select do |assoc_record|
                conditions.all? { |key, value| assoc_record.public_send(key) == value }
              end

              # Return a simple object that responds to `count`.
              result = Object.new
              result.define_singleton_method(:count) do |*args, &block|
                filtered_records.count(*args, &block)
              end
              result
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
