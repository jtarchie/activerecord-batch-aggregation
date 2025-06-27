# frozen_string_literal: true

require "benchmark/ips"
require "benchmark/memory"
require_relative "lib/active_record/eager/aggregation"

# Establish connection and define schema within the script
# to ensure it's self-contained.
ActiveRecord::Base.establish_connection({ adapter: "sqlite3", database: ":memory:" })

ActiveRecord::Schema.define do
  create_table :users, force: true do |t|
    t.string :name
  end

  create_table :posts, force: true do |t|
    t.string :title
    t.integer :user_id
  end
end

class User < ActiveRecord::Base
  has_many :posts
end

class Post < ActiveRecord::Base
  belongs_to :user
end

# --- Data Setup ---
puts "Setting up benchmark data..."
USER_COUNT = 100
POSTS_PER_USER = 10

User.transaction do
  USER_COUNT.times do |i|
    user = User.create!(name: "User #{i}")
    POSTS_PER_USER.times do |j|
      Post.create!(user: user, title: "Post #{j} for User #{i}")
    end
  end
end
puts "Setup complete. #{User.count} users, #{Post.count} posts."
puts "---"

# --- Benchmarking ---

puts "Running IPS benchmark..."
Benchmark.ips do |x|
  x.report("N+1 aggregation") do
    # Preload users to not measure the initial User.all query time
    users = User.all.to_a
    users.each { |user| user.posts.count }
  end

  x.report("eager_aggregations") do
    # Preload users with aggregations enabled
    users = User.eager_aggregations.to_a
    users.each { |user| user.posts.count }
  end

  x.compare!
end

puts "\nRunning Memory benchmark..."
Benchmark.memory do |x|
  x.report("N+1 aggregation memory") do
    users = User.all.to_a
    users.each { |user| user.posts.count }
  end

  x.report("eager_aggregations memory") do
    users = User.eager_aggregations.to_a
    users.each { |user| user.posts.count }
  end

  x.compare!
end
