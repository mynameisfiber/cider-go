```
           /.--
     ,--./,-. 
    / #      \
   | cider-go |
    \        / 
     `._,._,'
```

## What is cider-go?

Cider-go is a transparent redis proxy that provides the ability to use a cluster of fault tolerant redis databases with your normal client libraries.  It does this by understanding the redis protocol to speak to your client while delegating work out to a cluster of redis shards.  Each shard gets a piece of the possible keys so that load is (ideally) evenly distributed.  In addition, redundant shards can be added that will store the same data as their counterpart and act as a boost to read throughput (at the expense of more expensive write operations) and provide insurance in case a part of your cluster is damaged.

In summary, cider-go is awesome and does all the things you wish toast could.

## How do I use this?!

### As a transparent layer

#### Setup

It's easy!  First you have to build the `cider-go` application by running:

```
$ go build cider-go.go
```

And then you're ready to rumble!  If we had 4 redis servers (hostA, hostB, hostC and hostD), all running redis on port 1234, we could start `cider-go` with the following command:

```
$ ./cider-go --redis-group=hostA:1234:1,hostB:1234:1 --redis-group=hostC:1234:1,hostD:1234:1
```

This would have our key-space sharded into two groups, each of which is being replicated onto two hosts.  Gets on a key round-robin throughout the responsible group while writes are multicasted.

The configuration you use can be as flexible as you want.  In fact, the previous configuration could be changed to:

```
$ ./cider-go --redis-group=hostA:1234:1,hostB:1234:1,hostC:1234:1,hostD:1234:1 
```

In which case every host has the full dataset and reads are spread out, or

```
$ ./cider-go --redis-group=hostA:1234:1
               --redis-group=hostB:1234:1
               --redis-group=hostC:1234:1
               --redis-group=hostD:1234:1 
```

where every host has 25% of the dataset.

#### Usage

Since the `cider-go` layer understands the redis protocol and acts as a pass-through, nothing special needs to be done in order to use your new ultra-sharded redis cluster.  Simply connect to the `cider-go` using your usual redis client library and enjoy.  Some caveats are:

* You cannot have multiple databases.  This could be fixed in the future by having `cider-go` prepend keys with a pseudo-database, but it is highly unlikely
* Some commands are not supported.  This is mainly because they would not play nicely with keeping all shards in a shard-group synced up.  These commands are:
   * MGET (likely to be supported in the future)
   * MSET (likely to be supported in the future)
   * SPOP
   * RENAME
   * MOVE
   * RENAMENX
   * SDIFFSTORE
   * SELECT
   * SINTERSTORE
   * SMOVE
   * SUNION
   * SUNIONSTORE
   * ZINTERSTORE
   * ZUNIONSTORE


### As a library

The real core of the `cider-go` is the `cider-go/rediscluster` library.  It does low level translation of redis requests into their appropriate form for the current cluster configuration.  Simply setup a `RedisCluster` object, filled with the appropriate `RedisShardGroup`s, and you can use `RedisCluster.Do` to send `RedisMessage`s.

Please read the (hopefully) provided documentation for more information!

## Dependencies

* go1.0.2
* That's it... Really!

## Responses from the community

@dfm - AWWWWWWWWWWWW SHIIIIIIIIIIIIIIIIIIIT

@mreiferson - no way

@mynameisfiber - WIN.

## Future

* Fix up all the TODO's
* Support some of the currently unsupported commands
* MOOAR SPEED!
