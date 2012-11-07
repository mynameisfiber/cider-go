```
             ?Z                         
              ?                         
          ,~++I$+?+++===~=++:           
       ,=++=+??++=+++++=====+++         
      =?+?+???+?II+++++??+?+?7??=       
     ++I?I?????++?=????I?I??????+=      
    ++III??I??I????II777I?II??????,     
   =???++?III777I$$I7$77III???+II?+     
  ,?II+=+?7I77$$$$7I77$77IIIIIIIII+     
  =?++I=??7II7$7?7I$7777IIII???III?     
  =???IIII77 cider-go 777IIIII??II?     
  =+++???I77777$$7777$777II?IIIIII?     
  :+++++??II77777777777IIII???????+     
   +?+??+=777777$777777I77I??I????~     
   ~??++??II7I77$777777I7IIII?I??+      
    =?+?????77777777777I7II7I???+       
     +????II7777777I7777I7IIIIII        
      +I???II7$7777$7777777I7II         
       ?III?II777$$7$7I7777777          
        ~77II7I777$$$$$$$$$$+=~,        
         ~+$Z$Z$Z$ZZZZZODOZI+=          
           ~~===+???++=~~~,             
        __
        \/.--,
        //_.'
   .-""-/""-.
  /       __ \
 /        \\\ \
 |         || |
 \  cider-go  /
 \  \         /
  \  '-      /
   '-.__.__.'

     ,--./,-.
    / #      \
   | cider-go |
    \        / 
     `._,._,'
```

## What is cider-go?

Cider-go is a transparent redis proxy that provides the ability to use a cluster of fault taulerant redis databases with your normal client libraries.  It does this by understanding the redis protocol to speak to your client while delegating work out to a cluster of redis shards.  Each shard gets a piece of the possible keys so that load is (idealy) evenly distributed.  In addition, redundant shards can be added that will store the same data as their counterpart and act as a boost to read thoroughput (at the expense of more expensive write operations) and provide insurance incase a part of your cluster is damaged.

In summary, cider-go is awesome and does all the things you wish toast could.
