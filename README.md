# Hasura Client

A Hasura client for schema APIs https://hasura.io/docs/latest/graphql/core/api-reference/index.html

## Usage

### Create client

```go
client := hasura.NewClient(http.DefaultClient, "http://localhost:8080", "admin_secret")
```

### Query

#### Run SQL

```go
client.Query.RunSQL(source string, sql string, cascade bool) (io.ReadCloser, error)
```

**Example**

```go
reader, err := client.Query.RunSQL("default", "SELECT 1", false)

if err != nil {
  return err
}

// result
// { 
//   "result_type": "TuplesOk",
//   "result": [
//     ["?column?"],
//     ["1"]
//   ]
// }
```

#### Bulk requests

```go
client.Query.Bulk(args []*RequestBody) (io.ReadCloser, error)
```

**Example**

```go
reader, err := client.Query.Bulk([]*RequestBody{
  NewRunSQLRequest("default", "SELECT 1", false),
  NewRunSQLRequest("default", "SELECT NOW()", false),
})

if err != nil {
  return err
}

// result
// [{ 
//   "result_type": "TuplesOk",
//   "result": [
//     ["?column?"],
//     ["1"]
//   ]
// }, { 
//   "result_type": "TuplesOk",
//   "result": [
//     ["?column?"],
//     ["2021-11-01 15:04:05.999999+00"]
//   ]
// }]
```

Not all responses are necessary to be decoded, so the client doesn't automatically do it. Instead we separate the decoder into following utilities.

#### Unsafe decoder

The unsafe decoder uses reflect to decode query results to slice of structure objects.

```go
unsafe.Decode(r io.ReadCloser, v interface{}) error
// or
unsafe.DecodeBytes(input []byte, v interface{}) error
```

```go
var output []struct {
  Column1 string `column:"col1"`
  Column2 int    `column:"col2"`
}


err := Decode(reader, &output)
if err != nil {
  return err
}
```

For bulk request, the result are the list of raw table data. To decode multiple objects, use `DecodeBulk`.

```go
unsafe.DecodeBulk(r io.ReadCloser, v interface{}, args ...interface{}) error
// or
unsafe.DecodeBulkBytes(r io.ReadCloser, v interface{}, args ...interface{}) error
```

```go
var output1 []struct {
  Column1 string `column:"col1"`
  Column2 int    `column:"col2"`
}

var output2 []struct {
  Column3 int `column:"col3"`
  Column4 float64    `column:"col4"`
}

err := DecodeBulk(reader, &output1, output2)
if err != nil {
  return err
}
```