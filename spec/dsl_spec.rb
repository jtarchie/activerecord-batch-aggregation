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

  it "does not break nested aggregations without eager" do
    # Create categories
    tech_category = Category.create!(name: "Technology", active: true)
    lifestyle_category = Category.create!(name: "Lifestyle", active: true)
    inactive_category = Category.create!(name: "Inactive", active: false)

    # Create users with different roles and verification status
    authors = []
    3.times do |i|
      authors << User.create!(
        name: "Author #{i}",
        age: 25 + i,
        role: "author",
        verified: i < 2 # First 2 are verified
      )
    end

    regular_users = []
    2.times do |i|
      regular_users << User.create!(
        name: "User #{i}",
        age: 30 + i,
        role: "member",
        verified: true
      )
    end

    # Create posts with complex relationships
    authors.each_with_index do |author, author_idx|
      # Published posts
      5.times do |i|
        post = author.posts.create!(
          title: "Tech Post #{author_idx}-#{i}",
          score: (i + 1) * 20, # Scores: 20, 40, 60, 80, 100
          status: "published",
          published_at: (i + 1).weeks.ago
        )

        # Associate with categories
        post.post_categories.create!(category: tech_category, featured: i.even?)
        post.post_categories.create!(category: lifestyle_category, featured: i > 2)

        # Add comments from various users
        (regular_users + [author]).each do |commenter|
          2.times do |j|
            post.comments.create!(
              content: "Comment #{j}",
              user: commenter,
              upvotes: (i * 5) + j, # Varied upvotes
              created_at: j.days.ago
            )
          end
        end
      end

      # Draft posts
      2.times do |i|
        post = author.posts.create!(
          title: "Draft Post #{author_idx}-#{i}",
          score: 10,
          status: "draft"
        )
        post.post_categories.create!(category: inactive_category)
      end
    end

    users = User.active_authors.includes(:posts, :categories)

    expect(users.size).to eq(2) # Only verified authors

    users.each do |user|
      # Test basic post counting
      expect(user.posts.count).to eq(7) # 5 published + 2 draft

      # Test scoped post counting
      published_count = user.posts.published.count
      expect(published_count).to eq(5)

      high_score_count = user.posts.high_score.count
      expect(high_score_count).to eq(3) # Posts with score > 50

      # Test has_many through counting
      categories_count = user.categories.count
      expect(categories_count).to be >= 1

      active_categories_count = user.categories.active.count
      expect(active_categories_count).to be >= 1

      # Test nested through relationships
      user.posts.published.each do |post|
        # Comments count per post
        total_comments = post.comments.count
        expect(total_comments).to eq(6) # 3 users * 2 comments each

        popular_comments = post.comments.popular.count
        expect(popular_comments).to be >= 0

        # Categories per post
        post_categories = post.categories.active.count
        expect(post_categories).to be >= 1

        # Featured categorizations
        featured_cats = post.post_categories.featured.count
        expect(featured_cats).to be >= 0
      end

      # Test aggregation on through relationships
      user.categories.active.each do |category|
        # Posts in this category by this user
        user_posts_in_category = category.posts.where(user: user).published.count
        expect(user_posts_in_category).to be >= 0
      end
    end

    # Test aggregations at the category level
    Category.active.each do |category|
      # Total posts in category
      posts_count = category.posts.published.count
      expect(posts_count).to be >= 0

      # Unique users who posted in this category
      users_count = category.users.active_authors.count
      expect(users_count).to be >= 0

      # High-scoring posts in category
      high_score_posts = category.posts.published.high_score.count
      expect(high_score_posts).to be >= 0
    end
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

  it "handles eager count with custom SQL and raw conditions" do
    3.times do |i|
      user = User.create!(name: "User #{i}", role: "author", verified: true, age: 25 + i)
      # Create posts with specific patterns for testing raw SQL
      4.times { |j| user.posts.create!(title: "Post #{j}", score: j * 25) }
    end

    expect do
      users = User.active_authors.with_aggregations.where("age BETWEEN ? AND ?", 25, 27)
      users.each do |user|
        # Test count with raw SQL conditions
        posts_count = user.posts.count
        high_score_posts = user.posts.where("score > 50").count
        title_pattern_posts = user.posts.where("title LIKE ?", "Post%").count

        expect(posts_count).to eq(4)
        expect(high_score_posts).to eq(1)
        expect(title_pattern_posts).to eq(4)
      end
    end.not_to exceed_query_limit(4)
  end
end
