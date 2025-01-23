# mongodb_query_digest

## MongoDB 分析器

MongoDB 3.6 及以上的版本支持设置分析器的 sampleRate 参数，可以控制采样频率

### 检查 MongoDB 分析器是否开启

``` javascript
db.getProfilingStatus();
{
  was: 2,
  slowms: 100,
  sampleRate: 1,
  ok: 1
}
```


### 开启 MongoDB 分析器

``` javascript
db.setProfilingLevel(0)

db.system.profile.drop()

db.createCollection( "system.profile", { capped: true, size:4000000 } )

db.setProfilingLevel(2, { sampleRate: 0.1 })
```


## 运行 mongodb_query_digest

### 参数介绍



### 源代码运行

``` shell
go run main.go --host=11.158.242.22:27017 --database=test --typeinfo=true > test.log
```

### 二进制运行

``` shell

```
