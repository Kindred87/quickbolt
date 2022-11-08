# quickbolt

quickbolt provides a streamlined API for the creation and concurrent interaction of [bbolt](https://github.com/etcd-io/bbolt) databases.

# install
```
go get github.com/Kindred87/quickbolt
```

# quickstart

``` go
package main

import (
    "github.com/Kindred87/quickbolt"
)

func main() {
    db, err := quickbolt.Create("my_db.db")
    if err != nil {
        log.Fatalf("error while creating database: %s", err.Error())
    }

    defer db.RemoveFile()

    err := db.Insert([]byte("name"), []byte("quickbolt"), []string{"programs", "quickbolt"})
    if err != nil {
        log.Fatalf("error while writing to db: %s", err.Error())
    }

    b, err := db.GetValue([]byte("name"), []string{"programs", "quickbolt"}, true)
    if err != nil {
        log.Fatalf("error while getting name from db: %s", err.Error())
    }

    fmt.Printf("Got %s from db!\n", string(b))
}
```