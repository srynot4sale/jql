# JQL

## Create syntax

```
CREATE go to supermarket #todo #todo/completed

 #_db/id=@1
 #_db/content=go to supermarket
 #todo/completed
```


## Update syntax

```
@1 SET #todo/overdue

 #_db/id=@1
 #_db/content=go to supermarket
 #todo/completed
 #todo/overdue


@1 DEL #todo/completed

 #_db/id=@1
 #_db/content=go to supermarket
 #todo/overdue
```

Update all matches
```
#todo SET #todo/newprop
```


## GET syntax

```
@1

 Returns single match
```

```
#todo

 Returns all items with #todo tag
```


## Special meaning tags

```
"_" prefix for tags means it's internal

#_db - internal low-level properties
#_db/id - a unique ID for this item (@ff is a shortcut for #db/id=ff)
#_db/created - created time of item, can be overridden on creation
#_db/archived - when set, the item doesn't appear in search results unless archived is explicitly specified

#_tx - a transaction's metadata

```
