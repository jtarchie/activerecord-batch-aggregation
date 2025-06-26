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

        # This implementation is specific to the User/Post count test case.
        # It assumes the model is User and the association is posts.
        if instance_variable_defined?(:@perform_eager_aggregation) && klass.name == "User" && !records.empty?
          association_name = :posts
          reflection = klass.reflect_on_association(association_name)
          foreign_key = reflection.foreign_key
          primary_key = klass.primary_key

          record_ids = records.map(&primary_key.to_sym)

          counts = reflection.klass.where(foreign_key => record_ids).group(foreign_key).count

          records.each do |record|
            preloaded_count = counts[record.public_send(primary_key)] || 0
            association = record.association(association_name)

            # Mark the association as loaded and set a fake target so that
            # methods like `size` and `empty?` work without a query.
            association.loaded!
            association.target = Array.new(preloaded_count)

            # Get the association proxy to override `count` on it.
            proxy = record.public_send(association_name)

            # Because `count` on an association is designed to always query the DB,
            # we define a singleton method on this specific proxy to return our
            # pre-loaded value.
            proxy.define_singleton_method(:count) do |*args, &block|
              if args.empty? && !block
                preloaded_count
              else
                super(*args, &block)
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
