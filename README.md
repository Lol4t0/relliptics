relliptics
==========

Erlang binding for elliptics

##Usage

###Launck

Specify ```LD_LIBRARY_PATH``` to elliptics. For example, ```LD_LIBRARY_PATH=../priv erl```

###Connection

```{ok, DB} = relliptics:open([Options])```

####Options

* nodes

 ```{nodes, [<<"node1">>, {<<"node2">>, port2}, <<"node3">>]}``` - nodes to connect. Default ```[<<"localhost">>, 1025]```
 
* log_path

 ```{log_path <<"/dev/null">>}``` - where to write logs. Default ```{log_path <<"/dev/stderr">>}```
 
* namespace 
 
 ```{namespace, <<"ns">>}``` - namespace for keys. Deafult is empty (not set)
* nodes

 ```{nodes, [0, 1, 2]}``` - list of elliptics nodes. Deafult is ```{nodes, [0]}```
 
##Relliptics objects

* create

 ```Obj = relliptics:make_object(Key::binary(), Value::term())```
 ```Obj = relliptics:make_object(Key, Value, [Index1::binary(), Index2]```

* update

 ```
   Upd = relliptics:set_key(Obj, Key)
   Upd = relliptics:set_value(Obj, Value)
   Upd = relliptics:set_indexes(Obj, [Index11, Index22])
 ```

* query

 ```
   Key = relliptics:get_key(Obj)
   Value = relliptics:get_value(Obj)
   Indexes = relliptics:get_indexes(Obj)
 ```
 
##Write

  ```
    {ok, [UpdObject]} = relliptics:write(DB, [Object])
  ```
   
##Read

* by keys

  ```
    Acc = relliptics:fold(DB, Fun, Acc0, Keys)
    Fun(Obj::relliptics_objec(), Acc) -> Acc
   ```
* by indexes

  ```
    Acc = relliptics:fold_indexes(DB, Strategy, Fun, Acc0, Indexes)
  ```
  
  ```Startegy:```: ```any``` - return objects that have any of indexes specified. ```all``` - return objects that have all indexes specifies.




