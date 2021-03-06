# Database implementation on ruby.

## Description
```ruby
define_my_db(:my_database) do |db|
  db.create_table(:order) |table|
    table.autoincrement_primary_key 
    table.integer :department_id
    table.string :description, size: 10
    table.datetime :created_at 
     
    table.index :description
  end

  db.create_table(:comment) |table|
    table.autoincrement_primary_key
    table.integer :order_id
    table.string  :description, size: 10
    table.index :order_id, :hash 
  end
end

Order.find_by_id(3)                    # must be O(log(N))
Order.find_by(description: "anything") # must be O(log(N))
Comment.find_by(order_id: 4)           # must be O(1)
```


## Testing

`ruby ruby_db.rb`

## Results
*Restrictions:* Indices are being updated in RAM. To save it to filesystem flush_indices method must be used. 

![Order.create chart](https://raw.githubusercontent.com/solutus/play_db/master/creation_chart.png)

![Order.find_by chart](https://raw.githubusercontent.com/solutus/play_db/master/find_by_chart.png)
