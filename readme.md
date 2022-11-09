# quickbolt

quickbolt provides a streamlined API for the creation and concurrent interaction of [bbolt](https://github.com/etcd-io/bbolt) databases.

# Install
```
go get github.com/Kindred87/quickbolt
```

# Quickstart

``` go
package main

import (
    "log"
    "fmt"

    "github.com/Kindred87/quickbolt"
)

func main() {
    db, err := quickbolt.Create("my_db.db")
    if err != nil {
        log.Fatalf("error while creating database: %s", err.Error())
    }

    defer db.RemoveFile()

    err := db.Insert("name", "quickbolt", []string{"programs", "quickbolt"})
    if err != nil {
        log.Fatalf("error while writing to db: %s", err.Error())
    }

    b, err := db.GetValue("name", []string{"programs", "quickbolt"}, true)
    if err != nil {
        log.Fatalf("error while getting name from db: %s", err.Error())
    }

    fmt.Printf("Got %s from db!\n", string(b))
}
```

# Using buffer helpers

``` go
package main

import (
    "bytes"
    "log"
    "fmt"
    "os"

    "github.com/Kindred87/quickbolt"
    "golang.org/x/sync/errgroup"
)

func main() {

    db, err := quickbolt.Open("accounts.db")
    if err != nil {
        log.Fatalf("error while opening db: %s", err.Error())
    }

    defer db.Close()

    var eg errgroup.Group

    accountBuffer := make(chan []byte, 5)
    isClosedBuf := make(chan []byte, 5)
    captureBuf := make(chan []byte, 5)

    var closedAccounts []string

    // Iterate over all accounts, which are at the root of this database.
    eg.Go(func() error { return db.BucketsAt([][]byte{}, true, accountBuffer) })

    // Check each account if it is closed.  If so, pass it to the positive match buffer.
    eg.Go(func() error {
        return quickbolt.DoEach(accountBuffer, db, findClosedAccounts, isClosedBuf, 1000, nil, os.Stdout)
    })

    // Skip the Github account.
    eg.Go(func() error {
        return quickbolt.Filter(isCLosedBuf, captureBuf, func(b []byte) bool {return !bytes.Equal([]byte("Github"), b)}, nil, os.Stdout)
    })

    // Capture accounts passed through filter into a string slice (closedAccounts).
    eg.Go(func() error {
        return quickbolt.CaptureBytes(&closedAccounts, captureBuf, nil, nil, os.Stdout) 
    })

    // Wait for work to finish.
    if err := eg.Wait(); err != nil {
        return log.Fatalf("error while running report workers: %s", err.Error())
    }

    // Print all the closed accounts.
    for _, account := range closedAccounts {
        fmt.Println(account)
    }
}

// findClosedAccounts checks if the given account is closed.  If it is, the account is sent to the buffer.
func findClosedAccounts(account []byte, buffer chan []byte, db quickbolt.DB) error {
    v, err := db.GetValue("status", [][]byte{account}, true)
    if err != nil {
        return fmt.Errorf("error while getting status of account %s: %w", string(account), err)
    }

    if string(v) == "Closed" {
        err := quickbolt.Send(buffer, account, nil, os.Stdout)
        if err != nil {
            return fmt.Errorf("error while sending account %s to buffer: %w", string(account), err)
        }
    }
}
```