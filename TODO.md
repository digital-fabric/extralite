## Transforms

- Add ability to exclude a column:

```ruby
Extralite::Transform.new do
  {
    id: integer.identity,
    title: text,
    content: text,
    tags: [{
      id: integer.identity.skip,
      name: text
    }]
  }
end
#=>
[
  {
    id: 1,
    title: 'foo',
    content: 'bar',
    tags: [
      { name: 'baz' },
      { name: 'bug' }
    ]
  }
]
```

- Can we get rid of the hash container?

```ruby
Extralite::Transform.new do
  {
    id: integer.identity,
    title: text,
    content: text,
    _tag_id: skip,
    tags: [
      name: text
    ]
  }
end
#=>
[
  {
    id: 1,
    title: 'foo',
    content: 'bar',
    tags: [ 'baz', 'bug' ]
  }
]
```
