require 'set'
# TODO:
# indices
# coersion
module RubyDB
  class Database
    attr_reader :name, :tables
    def initialize(name)
      @name = name
      @tables = []
    end

    def create_table(name)
      table = RubyDBTable.new(name)
      yield table
      @tables << table
    end
  end

  class Field
    attr_reader :name
    def initialize(name, **options)
      @name = name
      @options = options
    end
  end

  class IdField < Field; end
  class IntegerField < Field; end
  class StringField < Field; end
  class DatetimeField < Field; end

  class IndexStruct
    ALLOWED_TYPES = %i[btree hash]
    attr_reader :name
    def initialize(name, type)
      check_index_type! type
      @name, @type = name, type
    end

    def check_index_type!(type)
      raise 'incorrect index name' unless ALLOWED_TYPES.include?(type)
    end
  end

  class RubyDBTable
    attr_reader :name, :autoincrement_primary_key, :indices
    def initialize(name)
      @name = name
      @integers = Set.new
      @strings = Set.new
      @datetimes = Set.new
      @indices = Set.new
      @autoincrement_primary_key = IdField.new(:id)
    end

    def fields_names
      fields.map(&:name)
    end

    def fields
      @fields ||= begin
        Set.new([@autoincrement_primary_key]) + @integers + @strings + @datetimes
      end
    end

    def autoincrement_primary_key(name = :id)
      @autoincrement_primary_key = IdField.new name
    end

    def integer(name)
      @integers << IntegerField.new(name)
    end

    def string(name, size: 255)
      @strings << StringField.new(name, size: size)
    end

    def datetime(name)
      @datetimes << DatetimeField.new(name)
    end

    def index(name, type = :btree)
      @indices << IndexStruct.new(name, type)
    end
  end

  class PersistentOperations
    require 'csv'

    def initialize(database, table)
      @table = table
      @storage = PersistentStorage.new(database.name, table.name, table.indices.map(&:name))
    end

    def find_by(options)
      id = @storage.id_from_indices(indices_data(options))
      return @storage.by_id(id) if id

      @storage.read_lines.find do |row|
        options.find do |field, value|
          row.fetch(field.to_s) == value
        end
      end
    end

    def create(attributes)
      @storage.append_row @table.fields_names, attributes
      @storage.add_indices indices_data(attributes), attributes[id_name]
    end

    def id_name
      @table.autoincrement_primary_key.name
    end

    def last_id
      @storage.last_id @table.fields_names, id_name
    end

    def indices_data(attributes)
      attributes.select { |field, value| @table.indices.map(&:name).include? field }
    end
  end

  class PersistentStorage
    def initialize(database_name, table_name, indices_fields)
      @database_name = database_name
      @table_name = table_name
      @indices_fields = indices_fields
    end

    def database_location
      self.class.database_location @database_name
    end

    def self.database_location(database_name)
      "#{database_name}-ruby-db"
    end

    def table_path
      "#{database_location}/#{@table_name}"
    end

    def rows_path
      "#{table_path}/rows"
    end

    def indices_path
      "#{table_path}/indices"
    end

    def last_id_path
      "#{table_path}/last_id"
    end

    def append_row(headers, attributes)
      attributes = attributes
      attrs_to_save = headers.map do |field|
        attributes[field]
      end
      CSV.open(file_path(attributes[:id]), 'w') do |csv|
        csv << headers
        csv << CSV::Row.new(headers, attrs_to_save)
      end

      File.write(last_id_path, attributes[:id])
    end

    # extract id from file name
    def last_id(headers, primary_key)
      File.read(last_id_path) if File.exist?(last_id_path)
    end

    def read_lines(&block)
      rows = []
      Dir["#{rows_path}/*"].each do |file|
        CSV.foreach(file, headers: true) { |row| rows << row }
      end
      rows.each(&block)
    end

    def add_indices(indices_data, id)
      indices_data.each { |field, value| indices[field].add(value, id) }
    end

    def file_path(id)
      "#{table_path}/rows/#{id}.csv"
    end

    def read_file(file)
      CSV.foreach(file, headers: true).to_a
    end

    def by_id(id)
      read_file(file_path(id)).first
    end

    def id_from_indices(options)
      return nil if options.empty?

      options.each do |field_name, value|
        id = indices[field_name].id value
        return id unless id.nil?
      end
      nil
    end

    def indices
      @indices ||= @indices_fields.each_with_object({}) do |field, memo|
        memo[field] = Index.new database_location, indices_path, field
      end
    end

    class Index
      def initialize(database_path, indices_path, field_name)
      binding.pry
        @database_path = database_path
        @indices_path = indices_path
        @field_name = field_name
      end

      def add(value, id)
        tree.add(value, id)
        flush_to_file
      end

      # returns id
      def id(value)
        tree.search(value).payload
      end

      private
      def tree
        @tree ||= Marshal.load tree_file_data
      end

      def tree_file_data
        puts "tree_file_data"
        return File.read(index_path) if File.exists?(index_path)

        init_tree
        flush_to_file
      end

      def init_tree
        @tree = RedBlackTree.new
      end

      def flush_to_file
        dump = tree_dump
        File.write(index_path, dump)
        dump
      end

      def index_path
        [@indices_path, @field_name].join "/"
      end

      def tree_dump
        Marshal.dump tree
      end
    end
  end

  module CreateDatabase
    def self.call(database)
      Dir.mkdir(PersistentStorage.database_location(database.name))

      database.tables.each do |table|
        storage = PersistentStorage.new(database.name, table.name, table.indices.map(&:name))
        Dir.mkdir storage.table_path
        Dir.mkdir storage.rows_path
        Dir.mkdir storage.indices_path
      end
    end
  end

  module DropDatabase
    def self.call(database_name)
      dir_name = PersistentStorage.database_location database_name
      return unless Dir.exists? dir_name
      FileUtils.rm_rf dir_name
    end
  end

  class CreateValidationException < Exception; end

  module Base
    def schema=(schema)
      @schema = schema
    end

    def table_name=(name)
      @table_name = name
    end

    def table_schema
      @table_schema ||= @schema.tables.find { |table| table.name == @table_name }
    end

    # attributes have symbol keys
    def create(attributes)
      validate_schema!(attributes)

      attributes = attributes
        .yield_self(&method(:set_timestamps))
        .yield_self(&method(:set_id))

      operations.create attributes
    end

    def find_by(attributes)
      validate_schema!(attributes)
      operations.find_by(attributes)
    end

    # validates names only
    def validate_schema!(attributes)
      diff = (attributes.keys - table_schema.fields_names)
      return if diff.size == 0
      raise CreateValidationException.new("fields #{diff.inspect} aren't found")
    end

    def set_timestamps(attributes)
      return attributes unless table_schema.fields_names.include?(:created_at)
      attributes.merge created_at: Time.now
    end

    def set_id(attributes)
      last_id = operations.last_id.to_i
      id = last_id + 1
      attributes.merge id: id
    end

    def operations
      @operations ||= PersistentOperations.new(@schema, table_schema)
    end
  end


end

def define_ruby_db(name)
  database = RubyDB::Database.new(name)
  yield database
  database
end

require 'pry-byebug'
def db
  define_ruby_db(:my_database) do |db|
    db.create_table(:orders) do |table|
      table.autoincrement_primary_key
      table.integer :department_id
      table.string :description, size: 10
      table.datetime :created_at

      table.index :description
    end

    db.create_table(:comments) do |table|
      table.autoincrement_primary_key
      table.integer :order_id
      table.string  :description, size: 10
      table.index :order_id, :hash
    end
  end
end

# https://github.com/headius/redblack
# Algorithm based on "Introduction to Algorithms" by Cormen and others
class RedBlackTree
  class Node
    attr_accessor :color
    attr_accessor :key
    attr_accessor :left
    attr_accessor :right
    attr_accessor :parent
    attr_reader   :payload

    RED = :red
    BLACK = :black
    COLORS = [RED, BLACK].freeze

    def initialize(key, payload, color = RED)
      raise ArgumentError, "Bad value for color parameter" unless COLORS.include?(color)
      @color = color
      @key = key
      @left = @right = @parent = NilNode.instance
      @payload = payload
    end

    def black?
      return color == BLACK
    end

    def red?
      return color == RED
    end
  end

  class NilNode < Node
    class << self
      private :new
      @instance = nil

      # it's not thread safe
      def instance
        if @instance.nil?
          @instance = new

          def instance
            return @instance
          end
        end

        return @instance
      end
    end

    def initialize
      self.color = BLACK
      self.key = 0
      self.left = nil
      self.right = nil
      self.parent = nil
    end

    def nil?
      return true
    end
  end

  include Enumerable

  attr_accessor :root
  attr_accessor :size

  def initialize
    self.root = NilNode.instance
    self.size = 0
  end

  def add(key, payload)
    insert(Node.new(key, payload))
  end

  def insert(x)
    insert_helper(x)

    x.color = Node::RED
    while x != root && x.parent.color == Node::RED
      if x.parent == x.parent.parent.left
        y = x.parent.parent.right
        if !y.nil? && y.color == Node::RED
          x.parent.color = Node::BLACK
          y.color = Node::BLACK
          x.parent.parent.color = Node::RED
          x = x.parent.parent
        else
          if x == x.parent.right
            x = x.parent
            left_rotate(x)
          end
          x.parent.color = Node::BLACK
          x.parent.parent.color = Node::RED
          right_rotate(x.parent.parent)
        end
      else
        y = x.parent.parent.left
        if !y.nil? && y.color == Node::RED
          x.parent.color = Node::BLACK
          y.color = Node::BLACK
          x.parent.parent.color = Node::RED
          x = x.parent.parent
        else
          if x == x.parent.left
            x = x.parent
            right_rotate(x)
          end
          x.parent.color = Node::BLACK
          x.parent.parent.color = Node::RED
          left_rotate(x.parent.parent)
        end
      end
    end
    root.color = Node::BLACK
  end

  alias << insert

  def delete(z)
    y = (z.left.nil? || z.right.nil?) ? z : successor(z)
    x = y.left.nil? ? y.right : y.left
    x.parent = y.parent

    if y.parent.nil?
      self.root = x
    else
      if y == y.parent.left
        y.parent.left = x
      else
        y.parent.right = x
      end
    end

    z.key = y.key if y != z

    if y.color == Node::BLACK
      delete_fixup(x)
    end

    self.size -= 1
    return y
  end

  def minimum(x = root)
    while !x.left.nil?
      x = x.left
    end
    return x
  end

  def maximum(x = root)
    while !x.right.nil?
      x = x.right
    end
    return x
  end

  def successor(x)
    if !x.right.nil?
      return minimum(x.right)
    end
    y = x.parent
    while !y.nil? && x == y.right
      x = y
      y = y.parent
    end
    return y
  end

  def predecessor(x)
    if !x.left.nil?
      return maximum(x.left)
    end
    y = x.parent
    while !y.nil? && x == y.left
      x = y
      y = y.parent
    end
    return y
  end

  def inorder_walk(x = root)
    x = self.minimum
    while !x.nil?
      yield x.key
      x = successor(x)
    end
  end

  alias each inorder_walk

  def reverse_inorder_walk(x = root)
    x = self.maximum
    while !x.nil?
      yield x.key
      x = predecessor(x)
    end
  end

  alias reverse_each reverse_inorder_walk

  def search(key, x = root)
    while !x.nil? && x.key != key
      key < x.key ? x = x.left : x = x.right
    end
    return x
  end

  def empty?
    return self.root.nil?
  end

  def black_height(x = root)
    height = 0
    while !x.nil?
      x = x.left
      height +=1 if x.nil? || x.black?
    end
    return height
  end

private

  def left_rotate(x)
    raise "x.right is nil!" if x.right.nil?
    y = x.right
    x.right = y.left
    y.left.parent = x if !y.left.nil?
    y.parent = x.parent
    if x.parent.nil?
      self.root = y
    else
      if x == x.parent.left
        x.parent.left = y
      else
        x.parent.right = y
      end
    end
    y.left = x
    x.parent = y
  end

  def right_rotate(x)
    raise "x.left is nil!" if x.left.nil?
    y = x.left
    x.left = y.right
    y.right.parent = x if !y.right.nil?
    y.parent = x.parent
    if x.parent.nil?
      self.root = y
    else
      if x == x.parent.left
        x.parent.left = y
      else
        x.parent.right = y
      end
    end
    y.right = x
    x.parent = y
  end

  def insert_helper(z)
    y = NilNode.instance
    x = root
    while !x.nil?
      y = x
      z.key < x.key ? x = x.left : x = x.right
    end
    z.parent = y
    if y.nil?
      self.root = z
    else
      z.key < y.key ? y.left = z : y.right = z
    end
    self.size += 1
  end

  def delete_fixup(x)
    while x != root && x.color == Node::BLACK
      if x == x.parent.left
        w = x.parent.right
        if w.color == Node::RED
          w.color = Node::BLACK
          x.parent.color = Node::RED
          left_rotate(x.parent)
          w = x.parent.right
        end
        if w.left.color == Node::BLACK && w.right.color == Node::BLACK
          w.color = Node::RED
          x = x.parent
        else
          if w.right.color == Node::BLACK
            w.left.color = Node::BLACK
            w.color = Node::RED
            right_rotate(w)
            w = x.parent.right
          end
          w.color = x.parent.color
          x.parent.color = Node::BLACK
          w.right.color = Node::BLACK
          left_rotate(x.parent)
          x = root
        end
      else
        w = x.parent.left
        if w.color == Node::RED
          w.color = Node::BLACK
          x.parent.color = Node::RED
          right_rotate(x.parent)
          w = x.parent.left
        end
        if w.right.color == Node::BLACK && w.left.color == Node::BLACK
          w.color = Node::RED
          x = x.parent
        else
          if w.left.color == Node::BLACK
            w.right.color = Node::BLACK
            w.color = Node::RED
            left_rotate(w)
            w = x.parent.left
          end
          w.color = x.parent.color
          x.parent.color = Node::BLACK
          w.left.color = Node::BLACK
          right_rotate(x.parent)
          x = root
        end
      end
    end
    x.color = Node::BLACK
  end
end

class Order
  extend RubyDB::Base
  self.schema = db
  self.table_name = :orders
end

def recreate_db
  database = db

  # initialize from scratch

  RubyDB::DropDatabase.call database.name
  RubyDB::CreateDatabase.call database
end

# testing
def create_orders(amount)
  amount.times.each do |i|
    Order.create(description: "order ##{i}", department_id: 1000 + i)
  end
end

def find_orders
	Order.find_by(description: "not found")
end

# # find data
# puts Order.find_by(id: 3)                    # сложность O(log(N))
# #Order.find_by(description: "anything") # сложность O(log(N))
# #Comment.find_by(order_id: 4) # сложность O(1)


require 'benchmark'

if false
  recreate_db
  #create_orders(3)
end

if false
	puts Order.find_by(description: "order #1999")
end

if false
  puts "creation testing ------------------"
  amounts = 13.times.map { |i|  2 ** i }
  amounts.each do |amount|
    recreate_db
    total = Benchmark.measure { create_orders(amount) }.total
    print amount.to_s.ljust(5)
    puts "%.3f" % total
  end
end

if true
  puts "find testing ------------------"
  amounts = 13.times.map { |i|  2 ** i }
  amounts.each do |amount|
    #recreate_db
    #create_orders(amount)

    total = Benchmark.measure { amount.times.each { find_orders } }.total
    print amount.to_s.ljust(5)
    puts "%.3f" % total
  end
end

if false
	puts Order.find_by(description: "order #1000")
end

