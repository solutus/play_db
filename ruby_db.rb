require 'set'
require_relative "red_black_tree.rb"
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
      indices_attrs = indices_data(options)
      if !indices_attrs.nil?
        id = @storage.id_from_indices(indices_attrs)
        return id ? @storage.by_id(id) : nil
      end

      @storage.read_lines.find do |row|
      #binding.pry
        options.find do |field, value|
          row.fetch(field.to_s) == value
        end
      end
    end

    def create(attributes)
      @storage.append_row @table.fields_names, attributes
      @storage.add_indices indices_data(attributes), attributes[id_name]
    end

    def flush_indices
      @storage.flush_indices
    end

    def id_name
      @id_name ||= @table.autoincrement_primary_key.name
    end

    def last_id
      @storage.last_id @table.fields_names, id_name
    end

    def indices_names
      @indices_names ||= @table.indices.map(&:name)
    end

    def indices_data(attributes)
      attributes.slice(*indices_names)
      #indices_names.each_with_object({}) { |field, memo| memo[field] = attributes[field] }
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
      @table_path ||= "#{database_location}/#{@table_name}"
    end

    def rows_path
      @rows_path ||= "#{table_path}/rows"
    end

    def indices_path
      @indices_path ||= "#{table_path}/indices"
    end

    def last_id_path
      @last_id_path ||= "#{table_path}/last_id"
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

    def flush_indices
      indices.values.each(&:flush_to_file)
    end

    class HashIndex
      def initialize(database_path, indices_path, field_name)
        @database_path = database_path
        @indices_path = indices_path
        @field_name = field_name
      end

      def add(value, id)
        tree[value] = id
      end

      # returns id
      def id(value)
        tree[value]
      end

      def flush_to_file
        dump = tree_dump
        File.write(index_path, dump)
        dump
      end

      private
      def tree
        @tree ||= begin
          Marshal.load tree_file_data
        end
      end

      def tree_file_data
        puts "tree_file_data"
        return File.read(index_path) if File.exists?(index_path)

        init_tree
        flush_to_file
      end

      def init_tree
        @tree = {}
      end

      def index_path
        [@indices_path, @field_name].join "/"
      end

      def tree_dump
        Marshal.dump tree
      end
    end

    class Index
      def initialize(database_path, indices_path, field_name)
        @database_path = database_path
        @indices_path = indices_path
        @field_name = field_name
      end

      def add(value, id)
        tree.add(value, id)
        #flush_to_file
      end

      # returns id
      def id(value)
        tree.search(value).payload
      end

      def flush_to_file
        dump = tree_dump
        File.write(index_path, dump)
        dump
      end

      private
      def tree
        @tree ||= begin
          puts "called once"
          Marshal.load tree_file_data
        end
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

    def flush_indices
      @operations.flush_indices
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
  #binding.pry
    index = i + 1 # the same as id
    Order.create(description: "order ##{index}", department_id: 1000 + index)
  end
  Order.flush_indices
end

def find_orders
	Order.find_by(description: "not found")
end

require 'benchmark'

if true
  recreate_db
  create_orders(3)
end

if false
	puts Order.find_by(description: "order #1999")
end

if true
  puts "creation testing ------------------"
  amounts = 16.times.map { |i|  2 ** i }
  amounts.each do |amount|
    recreate_db
    total = Benchmark.measure { create_orders(amount) }.total
    print amount.to_s.ljust(10)
    puts "\t%.3f" % total
  end
end

puts

if true
  puts "find testing ------------------"
  amounts = 16.times.map { |i|  2 ** i }
  amounts.each do |amount|
    recreate_db
    create_orders(amount)
    Order.flush_indices

    total = Benchmark.measure { 100000.times.each { find_orders } }.total
    print amount.to_s.ljust(10)
    puts "\t%.3f" % total
  end
end

if false
	puts Order.find_by(description: "order #1000")
end

