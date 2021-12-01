# Weighted Rendezvous Hashing

An alternative to Consistent Hashing.  Evenly distributes load on node removal.

```go
ring := rendezvous.New()
for _, s := range serversAndWeights {
  ring.AddWithWeight(s.Addr, s.Weight)
}

addr := ring.Lookup("some_client_addr")
```

Copyright (c) 2021 Minoru Osuka  
Copyright (c) 2020 Benjamin Bader

Made available under the Clear BSD License.
