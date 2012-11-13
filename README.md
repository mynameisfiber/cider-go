```
           /.--
     ,--./,-. 
    / #      \
   | cider-go |
    \        / 
     `._,._,'
```

## What is cider-go?

Cider-go is a transparent redis proxy that provides the ability to use a cluster of fault taulerant redis databases with your normal client libraries.  It does this by understanding the redis protocol to speak to your client while delegating work out to a cluster of redis shards.  Each shard gets a piece of the possible keys so that load is (idealy) evenly distributed.  In addition, redundant shards can be added that will store the same data as their counterpart and act as a boost to read thoroughput (at the expense of more expensive write operations) and provide insurance incase a part of your cluster is damaged.

In summary, cider-go is awesome and does all the things you wish toast could.

## Responses from the community

@dfm - AWWWWWWWWWWWW SHIIIIIIIIIIIIIIIIIIIT

@mreiferson - no way

@mynameisfiber - WIN.
