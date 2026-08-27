## Stmt caching

- We cache only parametric queries
- We need a cache flag to set when query parameters are given
- We need to pass that flag in order to tell `exec_multi_stmt` to not finalize the stmt
- When cache flag is set, we do the following:
  - lookup the stmt in the cache
  - if not found, go get it, then set cache
  - if found, run it
  - 

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

- Use single value instead of hash

```ruby
Extralite::Transform.new do
  {
    id: integer.identity,
    title: text,
    content: text,
    _tag_id: skip,
    tags: [
      { name: text.unbox }
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
