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

  class Index
    ALLOWED_TYPES = %i[btree hash]
    def initialize(name, type)
      check_index_type! type
      @name, @type = name, type
    end

    def check_index_type!(type)
      raise 'incorrect index name' unless ALLOWED_TYPES.include?(type)
    end
  end

  class RubyDBTable
    attr_reader :name
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
      @indices << Index.new(name, type)
    end
  end

  class PersistentOperations
    require 'csv'

    def initialize(database, table)
      @table = table
      @table_path = PersistentStorage.table_path(database, table)
    end

    def find_by(options)
      PersistentStorage.read_lines(@table_path).find do |row|
        options.find do |field, value|
          row.fetch(field.to_s) == value
        end
      end
    end

    # save attributes values sorted by attributes names
    def create(attributes)
      PersistentStorage.append_row @table_path, @table.fields_names, attributes
    end
  end

  module PersistentStorage
    class << self
      def database_location(database)
        "#{database.name}-ruby-db"
      end

      def table_file(table)
        "#{table.name}-ruby-db.csv"
      end

      def table_path(database, table)
        "#{database_location(database)}/#{table_file(table)}"
      end

      def append_row(path, headers, attributes)
        attributes = attributes
        attrs_to_save = headers.map do |field|
          attributes[field]
        end
        CSV.open(path, 'a') { |csv| csv << CSV::Row.new(headers, attrs_to_save) }
      end

      def append_headers(path, headers)
        CSV.open(path, 'a') { |csv| csv << headers.sort }
      end

      def read_lines(path, &block)
        CSV.foreach(path, headers: true, &block)
      end
    end
  end

  module CreateDatabase
    def self.call(database)
      Dir.mkdir(PersistentStorage.database_location(database))

      database.tables.each do |table|
        table_path = PersistentStorage.table_path(database, table)
        PersistentStorage.append_headers table_path, table.fields_names.sort
      end
    end
  end

  module DropDatabase
    def self.call(database)
      dir_name = PersistentStorage.database_location database
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
      PersistentOperations.new(@schema, @table_schema).create(attributes)
    end

    def find_by(attributes)
      validate_schema!(attributes)
      PersistentOperations.new(@schema, @table_schema).find_by(attributes)
    end

    # validates names only
    def validate_schema!(attributes)
      diff = (attributes.keys - table_schema.fields_names)
      return if diff.size == 0
      raise CreateValidationException.new("fields #{diff.inspect} aren't found")
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

# testing
database = db

# initialize from scratch
RubyDB::DropDatabase.call database
RubyDB::CreateDatabase.call database

# fill test data
2.times.each do |i|
  puts "create order: #{i}"
  Order.create(description: "order ##{i}", department_id: 1000 + i)
end

2.times.each do |i|
  puts "search order: #{i}"
  Order.find_by(description: "order ##{i}")
end

# find data
puts Order.find_by(id: 3)                    # сложность O(log(N))
#Order.find_by(description: "anything") # сложность O(log(N))
#Comment.find_by(order_id: 4) # сложность O(1)
