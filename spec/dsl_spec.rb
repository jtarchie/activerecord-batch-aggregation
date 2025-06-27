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
      t.string :role, default: "member"
      t.boolean :verified, default: false
    end

    model do
      has_many :posts
      has_many :comments
      has_many :categories, through: :posts
      has_many :published_posts, -> { published }, class_name: "Post"

      scope :verified, -> { where(verified: true) }
      scope :authors, -> { where(role: "author") }
      scope :active_authors, -> { authors.verified }
    end
  end

  with_model :Post do
    table do |t|
      t.string :title
      t.integer :score, default: 0
      t.belongs_to :user
      t.string :status, default: "draft"
      t.datetime :published_at
    end

    model do
      belongs_to :user
      has_many :post_categories
      has_many :categories, through: :post_categories
      has_many :comments

      scope :published, -> { where(status: "published") }
      scope :high_score, -> { where("score > ?", 50) }
      scope :recent, -> { where("published_at > ?", 1.month.ago) }
    end
  end

  with_model :Category do
    table do |t|
      t.string :name
      t.boolean :active, default: true
    end

    model do
      has_many :post_categories
      has_many :posts, through: :post_categories
      has_many :users, through: :posts

      scope :active, -> { where(active: true) }
      scope :popular, -> { joins(:posts).group("categories.id").having("COUNT(posts.id) > ?", 5) }
    end
  end

  with_model :PostCategory do
    table do |t|
      t.belongs_to :post
      t.belongs_to :category
      t.boolean :featured, default: false
    end

    model do
      belongs_to :post
      belongs_to :category

      scope :featured, -> { where(featured: true) }
    end
  end

  with_model :Comment do
    table do |t|
      t.text :content
      t.belongs_to :post
      t.belongs_to :user
      t.integer :upvotes, default: 0
      t.datetime :created_at
    end

    model do
      belongs_to :post
      belongs_to :user

      scope :popular, -> { where("upvotes > ?", 10) }
      scope :recent, -> { where("created_at > ?", 1.week.ago) }
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
      scope = User.with_aggregations.all
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
      scope = User.with_aggregations.where(age: 30..50)
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
      users = User.with_aggregations.all
      users.each do |user|
        even_posts_count = user.posts.where(title: "Even").count
        expect(even_posts_count).to eq(3)
      end
    end.not_to exceed_query_limit(3)
  end

  it "handles eager count with a scope" do
    5.times do |i|
      user = User.create!(name: "User #{i}", role: "author", verified: true)
      5.times { |j| user.posts.create!(title: j.even? ? "Even" : "Odd") }
    end
    5.times do |i|
      user = User.create!(name: "User #{i}", role: "author", verified: false)
      5.times { |j| user.posts.create!(title: j.even? ? "Even" : "Odd") }
    end
    5.times do |i|
      user = User.create!(name: "User #{i}", role: "not_author", verified: true)
      5.times { |j| user.posts.create!(title: j.even? ? "Even" : "Odd") }
    end

    expect do
      users = User.active_authors.with_aggregations
      expect(users.size).to eq(5)
      users.each do |user|
        even_posts_count = user.posts.where(title: "Even").count
        expect(even_posts_count).to eq(3)
      end
    end.not_to exceed_query_limit(3)
  end

  it "handles has_many through associations with eager loading" do
    3.times do |i|
      user = User.create!(name: "User #{i}", role: "author", verified: i.even?)
      category = Category.create!(name: "Category #{i}", active: i.odd?)
      2.times { |j| category.posts << user.posts.create!(title: "Post #{j}") }
    end

    expect do
      users = User.active_authors.with_aggregations
      expect(users.size).to eq(2) # No active authors created yet

      users.each do |user|
        categories_count = user.categories.count
        expect(categories_count).to eq(2) # Each user has 2 categories
      end
    end.not_to exceed_query_limit(3)
  end

  it "handles eager count with nested scopes and conditions" do
    5.times do |i|
      user = User.create!(name: "User #{i}", role: "author", verified: true, age: 25 + i)
      # Mix of published and draft posts with varying scores
      4.times { |j| user.posts.create!(title: "Post #{j}", status: "published", score: j * 30, published_at: j.days.ago) }
      2.times { |j| user.posts.create!(title: "Draft #{j}", status: "draft", score: 10) }
    end

    expect do
      users = User.active_authors.with_aggregations.where(age: 25..28)
      users.each do |user|
        # Test multiple scoped counts
        published_count = user.posts.published.count
        high_score_count = user.posts.high_score.count
        recent_count = user.posts.recent.count

        expect(published_count).to eq(4)
        expect(high_score_count).to eq(2) # scores 60, 90
        expect(recent_count).to be >= 0
      end
    end.not_to exceed_query_limit(4) # May need one extra for complex scopes
  end

  it "handles eager count with empty results" do
    # Create users but no posts
    3.times { |i| User.create!(name: "User #{i}", role: "author", verified: true) }

    expect do
      users = User.active_authors.with_aggregations
      users.each do |user|
        expect(user.posts.count).to eq(0)
        expect(user.categories.count).to eq(0)
        expect(user.comments.count).to eq(0)
      end
    end.not_to exceed_query_limit(4)
  end

  it "handles eager count with complex join conditions" do
    user = User.create!(name: "Test User", role: "author", verified: true)
    tech_cat = Category.create!(name: "Tech", active: true)
    lifestyle_cat = Category.create!(name: "Lifestyle", active: false)

    post1 = user.posts.create!(title: "Tech Post", status: "published")
    post2 = user.posts.create!(title: "Life Post", status: "published")

    post1.post_categories.create!(category: tech_cat, featured: true)
    post2.post_categories.create!(category: lifestyle_cat, featured: false)

    expect do
      users = User.active_authors.with_aggregations
      users.each do |user|
        # Count through associations with conditions
        active_categories = user.categories.active.count
        featured_posts = user.posts.joins(:post_categories).where(post_categories: { featured: true }).count

        expect(active_categories).to eq(1)
        expect(featured_posts).to eq(1)
      end
    end.not_to exceed_query_limit(4)
  end

  it "handles eager count with polymorphic or complex associations" do
    # Test with comments (user has_many comments directly)
    users = []
    3.times do |i|
      user = User.create!(name: "User #{i}", role: "author", verified: true)
      users << user

      # Create posts and comments
      2.times do |j|
        post = user.posts.create!(title: "Post #{j}")
        # User comments on their own posts
        ((j + 1) * 2).times { |k| post.comments.create!(content: "Comment #{k}", user: user, upvotes: k * 5) }
      end
    end

    expect do
      eager_users = User.active_authors.with_aggregations
      eager_users.each do |user|
        posts_count = user.posts.count
        comments_count = user.comments.count
        popular_comments = user.comments.popular.count

        expect(posts_count).to eq(2)
        expect(comments_count).to eq(6)
        expect(popular_comments).to eq(1) # Comments with upvotes > 10
      end
    end.not_to exceed_query_limit(4)
  end

  it "handles eager count with order and limit on associations" do
    user = User.create!(name: "Test User", role: "author", verified: true)

    # Create posts with different scores and dates
    posts = []
    5.times do |i|
      posts << user.posts.create!(
        title: "Post #{i}",
        score: i * 20,
        status: "published",
        published_at: i.days.ago
      )
    end

    expect do
      users = User.active_authors.with_aggregations
      users.each do |user|
        # Test that ordering doesn't break count
        total_posts = user.posts.count
        recent_posts = user.posts.order(:published_at).count
        high_score_posts = user.posts.order(score: :desc).where("score > ?", 40).count

        expect(total_posts).to eq(5)
        expect(recent_posts).to eq(5)
        expect(high_score_posts).to eq(2)
      end
    end.not_to exceed_query_limit(4)
  end

  it "handles eager count with aggregation functions beyond count" do
    user = User.create!(name: "Test User", role: "author", verified: true)

    5.times do |i|
      post = user.posts.create!(title: "Post #{i}", score: (i + 1) * 10, status: "published")
      3.times { |j| post.comments.create!(content: "Comment #{j}", user: user, upvotes: j * 2) }
    end

    expect do
      users = User.active_authors.with_aggregations
      users.each do |user|
        # While testing count specifically, ensure other aggregations don't interfere
        posts_count = user.posts.count
        total_score = user.posts.sum(:score)
        avg_score = user.posts.average(:score)
        max_score = user.posts.maximum(:score)

        expect(posts_count).to eq(5)
        expect(total_score).to eq(150) # 10+20+30+40+50
        expect(avg_score).to eq(30.0)
        expect(max_score).to eq(50)
      end
    end.not_to exceed_query_limit(5)
  end

  it "handles eager count with multiple association paths" do
    user = User.create!(name: "Test User", role: "author", verified: true)
    category = Category.create!(name: "Test Category", active: true)

    # Create a post that belongs to category and has comments
    post = user.posts.create!(title: "Test Post", status: "published")
    post.post_categories.create!(category: category)

    # Other users comment on the post
    2.times do |i|
      commenter = User.create!(name: "Commenter #{i}")
      post.comments.create!(content: "Comment #{i}", user: commenter)
    end

    expect do
      users = User.active_authors.with_aggregations
      users.each do |user|
        # Count through different association paths
        posts_count = user.posts.count
        categories_count = user.categories.count
        post_comments_count = user.posts.joins(:comments).count

        expect(posts_count).to eq(1)
        expect(categories_count).to eq(1)
        expect(post_comments_count).to eq(2) # Post has 2 comments
      end
    end.not_to exceed_query_limit(4)
  end

  it "handles eager calculations with named columns" do
    3.times do |i|
      user = User.create!(name: "User #{i}", role: "author", verified: true, age: 25 + (i * 5))
      # Create posts with varying scores
      5.times { |j| user.posts.create!(title: "Post #{j}", score: (j + 1) * 10) }
    end

    expect do
      users = User.active_authors.with_aggregations
      users.each do |user|
        # Test different calculation methods with column names
        posts_count = user.posts.count(:id)
        avg_score = user.posts.average(:score)
        min_score = user.posts.minimum(:score)
        max_score = user.posts.maximum(:score)
        total_score = user.posts.sum(:score)

        expect(posts_count).to eq(5)
        expect(avg_score).to eq(30.0) # (10+20+30+40+50)/5
        expect(min_score).to eq(10)
        expect(max_score).to eq(50)
        expect(total_score).to eq(150)
      end
    end.not_to exceed_query_limit(6) # One query per calculation type
  end

  it "handles eager calculations with custom SQL expressions" do
    2.times do |i|
      user = User.create!(name: "User #{i}", role: "author", verified: true, age: 30 + i)
      # Create posts with scores that relate to user age for testing custom expressions
      3.times { |j| user.posts.create!(title: "Post #{j}", score: (j + 1) * user.age) }
    end

    expect do
      users = User.active_authors.with_aggregations
      users.each_with_index do |user, i|
        # Test custom SQL calculations combining user and post data
        posts_count = user.posts.count
        score_age_sum = user.posts.sum("score + #{User.table_name}.age")
        avg_score_ratio = user.posts.average("score / #{User.table_name}.age")

        expect(posts_count).to eq(3)

        # For user 0 (age 30): posts have scores 30, 60, 90
        # score_age_sum = (30+30) + (60+30) + (90+30) = 60 + 90 + 120 = 270
        # For user 1 (age 31): posts have scores 31, 62, 93
        # score_age_sum = (31+31) + (62+31) + (93+31) = 62 + 93 + 124 = 279
        expected_sum = i == 0 ? 270 : 279
        expect(score_age_sum).to eq(expected_sum)
        expect(avg_score_ratio).to eq(2.0) # All ratios are score/age = multiple/1 = 2.0
      end
    end.not_to exceed_query_limit(5)
  end
end
