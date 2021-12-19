# JQL

## Create syntax

```
CREATE go to supermarket #todo #todo/completed

 #db/id=@1
 #db/content=go to supermarket
 #todo/completed
```


## Update syntax

```
@1 SET #todo/overdue

 #db/id=@1
 #db/content=go to supermarket
 #todo/completed
 #todo/overdue


@1 DEL #todo/completed

 #db/id=@1
 #db/content=go to supermarket
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
