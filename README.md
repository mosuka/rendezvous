# Weighted Rendezvous Hashing

An alternative to Consistent Hashing.  Evenly distributes load on node removal.

```go
ring := rendezvous.New()
for _, s := range serversAndWeights {
  ring.AddWithWeight(s.Addr, s.Weight)
}

addr := ring.Lookup("some_client_addr")
```
