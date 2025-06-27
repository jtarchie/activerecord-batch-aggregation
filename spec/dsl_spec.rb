# frozen_string_literal: true

require "active_record"
require "rspec-sqlimit"
require "spec_helper"
require "active_record/eager/aggregation"

RSpec.describe "ActiveRecord::Eager::Aggregation" do
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

  describe "standard ActiveRecord behavior" do
    before do
      10.times { |i| User.create!(name: "User #{i}", age: rand(1..100)) }
    end

    it "performs standard count queries efficiently" do
      expect { User.count }.not_to exceed_query_limit(1)
      expect(User.count).to eq(10)

      expect { User.where(name: "User 1").count }.not_to exceed_query_limit(1)
      expect(User.where(name: "User 1").count).to eq(1)
    end
  end

  describe "basic has_many associations" do
    before do
      @users = 5.times.map do |i|
        user = User.create!(name: "User #{i}", age: 20 + i)
        5.times { |j| user.posts.create!(title: j.even? ? "Even" : "Odd") }
        user
      end
    end

    it "efficiently loads count aggregations for has_many associations" do
      expect do
        users = User.eager_aggregations.all
        expect(users.size).to eq(5)

        users.each do |user|
          expect(user.posts.count).to eq(5)
          expect(user.posts.where(title: "Even").count).to eq(3)
        end
      end.not_to exceed_query_limit(4)
    end
  end

  describe "scopes and where conditions" do
    before do
      # Create a mix of authors and non-authors, verified and unverified
      5.times { |i| User.create!(name: "Author #{i}", role: "author", verified: true) }
      5.times { |i| User.create!(name: "Author #{i}", role: "author", verified: false) }
      5.times { |i| User.create!(name: "Member #{i}", role: "member", verified: true) }

      # Add posts for all users
      User.find_each do |user|
        5.times { |j| user.posts.create!(title: "Post #{j}", status: j < 2 ? "published" : "draft") }
      end
    end

    it "respects base scope when loading aggregations" do
      expect do
        users = User.active_authors.eager_aggregations
        expect(users.size).to eq(5)

        users.each do |user|
          expect(user.posts.count).to eq(5)
          expect(user.role).to eq("author")
          expect(user.verified).to be true
        end
      end.not_to exceed_query_limit(3)
    end

    it "handles chained scopes and conditions" do
      expect do
        users = User.active_authors.eager_aggregations.where("name LIKE ?", "%1%")

        users.each do |user|
          expect(user.posts.published.count).to eq(2)
          expect(user.role).to eq("author")
          expect(user.verified).to be true
        end
      end.not_to exceed_query_limit(3)
    end
  end

  describe "complex associations and calculations" do
    before do
      # Setup complex relationships for testing
      @author = User.create!(name: "Main Author", role: "author", verified: true, age: 35)

      # Create categories
      @tech = Category.create!(name: "Technology", active: true)
      @health = Category.create!(name: "Health", active: true)
      @finance = Category.create!(name: "Finance", active: false)

      # Create posts with varying scores and associate with categories
      3.times do |i|
        post = @author.posts.create!(
          title: "Tech Post #{i}",
          status: "published",
          score: (i + 1) * 30,
          published_at: i.days.ago
        )
        post.post_categories.create!(category: @tech, featured: i.zero?)

        # Add comments to posts
        3.times do |j|
          post.comments.create!(
            content: "Comment #{j}",
            user: @author,
            upvotes: j * 5
          )
        end
      end

      # Add health posts
      2.times do |i|
        post = @author.posts.create!(
          title: "Health Post #{i}",
          status: "published",
          score: i * 25
        )
        post.post_categories.create!(category: @health)
      end

      # Add one draft post
      draft = @author.posts.create!(title: "Draft", status: "draft")
      draft.post_categories.create!(category: @finance)
    end

    it "handles has_many through associations" do
      expect do
        users = User.active_authors.eager_aggregations

        users.each do |user|
          # Test different association paths
          expect(user.posts.count).to eq(6)
          expect(user.categories.count).to eq(3)
          expect(user.categories.active.count).to eq(2)
        end
      end.not_to exceed_query_limit(4)
    end

    it "handles various aggregation functions" do
      expect do
        users = User.active_authors.eager_aggregations

        users.each do |user|
          # Count
          expect(user.posts.published.count).to eq(5)
          expect(user.posts.high_score.count).to eq(2) # Two posts > 50 (60, 90)

          # Sum, avg, min, max
          expect(user.posts.sum(:score)).to eq(205) # 30+60+90+0+25+0
          expect(user.posts.average(:score).to_i).to eq(34) # 205 / 6 = ~34.17
          expect(user.posts.maximum(:score)).to eq(90)
          expect(user.posts.minimum(:score)).to eq(0)

          # Comments
          expect(user.comments.count).to eq(9) # 3 comments on 3 posts
          expect(user.comments.popular.count).to eq(0) # No comments with upvotes > 10
        end
      end.not_to exceed_query_limit(9)
    end

    it "handles joins and complex conditions" do
      expect do
        users = User.active_authors.eager_aggregations

        users.each do |user|
          featured_count = user.posts.joins(:post_categories).where(post_categories: { featured: true }).count
          tech_posts_count = user.posts.joins(:categories).where(categories: { name: "Technology" }).count

          expect(featured_count).to eq(1)
          expect(tech_posts_count).to eq(3)
        end
      end.not_to exceed_query_limit(3)
    end
  end

  describe "edge cases" do
    it "handles empty result sets" do
      User.create!(name: "Lonely User", role: "author", verified: true)

      expect do
        users = User.active_authors.eager_aggregations
        users.each do |user|
          expect(user.posts.count).to eq(0)
          expect(user.categories.count).to eq(0)
          expect(user.comments.count).to eq(0)
        end
      end.not_to exceed_query_limit(4)
    end

    it "handles custom SQL expressions in aggregations" do
      user = User.create!(name: "SQL User", role: "author", verified: true, age: 40)
      3.times { |i| user.posts.create!(title: "Post #{i}", score: (i + 1) * 10) }

      expect do
        users = User.active_authors.eager_aggregations
        users.each do |user|
          # Test with custom SQL
          weighted_sum = user.posts.sum("score * 2")
          expect(weighted_sum).to eq(120) # (10+20+30)*2
        end
      end.not_to exceed_query_limit(3)
    end
  end

  describe "async aggregations" do
    before do
      @user = User.create!(name: "Async User", role: "author", verified: true)
      5.times { |i| @user.posts.create!(title: "Post #{i}", score: (i + 1) * 10) }
    end

    it "supports async aggregation methods" do
      expect do
        users = User.where(id: @user.id).eager_aggregations
        user = users.first

        # Get promises
        posts_count_promise = user.posts.async_count
        high_score_posts_count_promise = user.posts.high_score.async_count
        score_sum_promise = user.posts.async_sum(:score)
        score_avg_promise = user.posts.async_average(:score)
        score_max_promise = user.posts.async_maximum(:score)
        score_min_promise = user.posts.async_minimum(:score)

        # Assert values from promises
        expect(posts_count_promise.value).to eq(5)
        expect(high_score_posts_count_promise.value).to eq(0) # score > 50
        expect(score_sum_promise.value).to eq(150) # 10+20+30+40+50
        expect(score_avg_promise.value.to_i).to eq(30) # 150 / 5
        expect(score_max_promise.value).to eq(50)
        expect(score_min_promise.value).to eq(10)
      end.not_to exceed_query_limit(7) # 1 for user, 6 for aggregations
    end
  end

  context "when used with eager_aggregations" do
    it "does not return records for non-aggregation calls" do
      user = User.create!
      user.posts.create!

      user_with_aggregation = User.eager_aggregations.find(user.id)

      posts_association = user_with_aggregation.posts
      expect(posts_association).to be_a(ActiveRecord::Eager::Aggregation::AggregationProxy)

      result = posts_association.first
      expect(result).not_to be_a(Post)
      expect(result).to be_a(ActiveRecord::Eager::Aggregation::AggregationProxy)

      loaded_relation = posts_association.to_a
      expect(loaded_relation).not_to be_an(Array)
      expect(loaded_relation).to be_a(ActiveRecord::Eager::Aggregation::AggregationProxy)

      user_without_aggregation = User.find(user.id)

      posts_association = user_without_aggregation.posts
      expect(posts_association).to be_a(ActiveRecord::Relation)

      result = posts_association.first
      expect(result).to be_a(Post)

      loaded_relation = posts_association.to_a
      expect(loaded_relation).to be_an(Array)
    end
  end

  describe "batch loading with eager_aggregations" do
    before do
      10.times do |i|
        user = User.create!(name: "Batch User #{i}", role: "author", verified: true)
        3.times { |j| user.posts.create!(title: "Post #{j}") }
      end
    end

    it "efficiently loads associations with find_in_batches" do
      expect do
        User.eager_aggregations.find_in_batches(batch_size: 5) do |batch|
          batch.each do |user|
            expect(user.posts.count).to eq(3)
          end
        end
      end.not_to exceed_query_limit(5) # 2 batches + 2 aggregation queries + 1 for setup
    end

    it "efficiently loads associations with find_each" do
      expect do
        User.eager_aggregations.find_each(batch_size: 4) do |user|
          expect(user.posts.count).to eq(3)
        end
      end.not_to exceed_query_limit(4) # 3 batches + 1 aggregation query
    end

    context "with has_many :through associations" do
      before do
        # Create categories and associate them with posts
        @categories = 3.times.map { |i| Category.create!(name: "Category #{i}") }

        User.find_each do |user|
          user.posts.each_with_index do |post, idx|
            # Associate each post with a different category
            post.post_categories.create!(category: @categories[idx % 3])
          end
        end
      end

      it "efficiently loads has_many :through associations with find_each" do
        expect do
          User.eager_aggregations.find_each(batch_size: 4) do |user|
            expect(user.categories.count).to eq(3)
          end
        end.not_to exceed_query_limit(7) # 3 batches + aggregation queries
      end

      it "efficiently loads has_many :through associations with find_in_batches" do
        expect do
          User.eager_aggregations.find_in_batches(batch_size: 5) do |batch|
            batch.each do |user|
              expect(user.categories.count).to eq(3)
            end
          end
        end.not_to exceed_query_limit(6) # 2 batches + aggregation queries
      end
    end
  end

  context "with block-based aggregations" do
    before do
      2.times do |i|
        user = User.create!(name: "Author #{i}", role: "author", verified: true)
        3.times { |j| user.posts.create!(score: (j + 1) * 10) }
      end
    end

    it "falls back to original sum and async_sum and causes N+1 queries" do
      # This is expected to be inefficient, but functionally correct.
      expect do
        users = User.active_authors.eager_aggregations.to_a
        users.each do |user|
          # block-based sum loads the association
          sum_in_ruby = user.posts.sum(&:score)
          expect(sum_in_ruby).to eq(60) # 10 + 20 + 30
        end
      end.to exceed_query_limit(2) # 1 for users, 2 for posts (N=2)
    end
  end
end
