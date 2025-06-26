# frozen_string_literal: true

require "active_record"
require "rspec-sqlimit"
require "spec_helper"
require_relative "../lib/activerecord/batch/aggregation"

RSpec.describe "DSL" do
  before(:all) do
    ActiveRecord::Base.establish_connection({ adapter: "sqlite3", database: ":memory:" })
    ActiveRecord::Base.connection
  end

  with_model :User do
    table do |t|
      t.string :name
      t.integer :age
    end

    model do
      has_many :posts
    end
  end

  with_model :Post do
    table do |t|
      t.string :title
      t.integer :score, default: 0
      t.belongs_to :user
    end
    model do
      belongs_to :user
    end
  end

  it "handles standard aggregation" do
    10.times { |i| User.create!(name: "User #{i}", age: rand(1..100)) }

    expect { User.count }.not_to exceed_query_limit(1)
    expect(User.count).to eq(10)

    expect { User.where(name: "User 1").count }.not_to exceed_query_limit(1)
    expect(User.where(name: "User 1").count).to eq(1)
  end

  it "handles eager count aggregation" do
    10.times do |i|
      user = User.create!(name: "User #{i}", age: rand(1..100))
      10.times { |j| user.posts.create!(title: "Post #{j}") }
    end

    expect do
      scope = User.eager.all
      expect(scope.size).to eq(10)

      scope.each do |user|
        expect(user.posts.count).to eq(10)
      end
    end.not_to exceed_query_limit(3)
  end

  it "handles eager count aggregation with where clause" do
    10.times do |i|
      user = User.create!(name: "User #{i}", age: i.even? ? 30 : 60) # Alternate ages for testing
      10.times { |j| user.posts.create!(title: "Post #{j}") }
    end

    expect do
      scope = User.eager.where(age: 30..50)
      expect(scope.size).to eq(5) # Adjust based on created users' ages

      scope.each do |user|
        expect(user.posts.count).to eq(10)
      end
    end.not_to exceed_query_limit(3)
  end

  it "handles counting posts with a specific condition per user" do
    5.times do |i|
      user = User.create!(name: "User #{i}", age: 20 + i)
      5.times { |j| user.posts.create!(title: j.even? ? "Even" : "Odd") }
    end

    expect do
      users = User.eager.all
      users.each do |user|
        even_posts_count = user.posts.where(title: "Even").count
        expect(even_posts_count).to eq(3)
      end
    end.not_to exceed_query_limit(3)
  end
end
