%%%-------------------------------------------------------------------
%%% @author sidorov
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. Jan 2014 11:16 AM
%%%-------------------------------------------------------------------
-module(relliptics).
-author("sidorov").

%% API
-export([open/1, close/1, write/2, fold/4, fold_indexes/5]).
-export([make_object/2, make_object/3, get_key/1, get_value/1, get_indexes/1, set_key/2, set_value/2, set_indexes/2]).

-record(relliptics_object, {key, value, saved_indexes = [], new_indexes = undefined}).

open(Config) ->
    case relliptics_nif:open(Config) of
        {ok, Conn} ->
            {ok, {make_ref(), Conn}};
        Else ->
            Else
    end.

close({_Ref, Conn}) ->
    relliptics_nif:close(Conn).



write({_Ref, Conn}, Data) when is_list(Data) ->
    {Serialized, Updated} = lists:foldl(fun serialize/2, {[], []}, Data),
    Ref = make_ref(),
    case relliptics_nif:write_async(Ref, Conn, Serialized) of
        ok ->
            receive
                {Ref, ok} ->
                    {ok, Updated};
                {Ref, Else} ->
                    Else
            end
    end;
write(DB, Object = #relliptics_object{}) ->
    write(DB, [Object]).

serialize(Object = #relliptics_object{key = Key, new_indexes = undefined}, {Serialized, Updated}) ->
    {[{Key, term_to_binary(Object)}|Serialized], [Object|Updated]};
serialize(Object = #relliptics_object{saved_indexes = CurrentIndexes, key = Key, new_indexes = Indexes},
          {Serialized, Updated}) when is_list(Indexes) ->
    IndexesToSave = Indexes -- CurrentIndexes,
    IndexesToRemove = CurrentIndexes -- Indexes,
    NewObject = Object#relliptics_object{saved_indexes = Indexes, new_indexes = undefined},
    {[{Key, term_to_binary(NewObject), IndexesToSave, IndexesToRemove}|Serialized], [NewObject|Updated]}.


fold({_Ref, Conn}, Fun, Acc, Keys) when is_list(Keys)->
    Ref = make_ref(),
    case relliptics_nif:read_async(Ref, Conn, Keys) of
        ok ->
            do_fold(Ref, Fun, Acc);
        Else ->
            {fold_error, Acc, Else}
    end.

do_fold(Ref, Fun, Acc) ->
    receive
        {Ref, {data, Object}} ->
            Acc1 = Fun(binary_to_term(Object), Acc),
            do_fold(Ref, Fun, Acc1);
        {Ref, ok} ->
            Acc;
        {Ref, Else} ->
            {do_fold_error, Acc, Else}
    end.


fold_indexes({_Ref, Conn}, Strategy, Fun, Acc, Indexes) when is_list(Indexes) ->
    Ref = make_ref(),
    case relliptics_nif:read_by_indexes_async(Ref, Conn, Strategy, Indexes) of
        ok ->
            do_fold(Ref, Fun, Acc);
        Else ->
            Else
    end.

make_object(Key, Body) when is_binary(Key) ->
    #relliptics_object{key = Key, value = Body}.



make_object(Key, Body, Indexes) ->
    Obj = make_object(Key, Body),
    set_indexes(Obj, Indexes).

get_key(#relliptics_object{key = Key}) ->
    Key.

get_value(#relliptics_object{value = Body}) ->
    Body.


get_indexes(#relliptics_object{new_indexes = Indexes}) when is_list(Indexes) ->
    Indexes;
get_indexes(#relliptics_object{saved_indexes = Indexes, new_indexes = undefined}) when is_list(Indexes) ->
    Indexes.

set_key(Obj, Key) when is_binary(Key)->
    Obj#relliptics_object{key = Key}.

set_value(Obj, Body) ->
    Obj#relliptics_object{value = Body}.



set_indexes(Obj, Indexes) when is_list(Indexes) ->
    Obj#relliptics_object{new_indexes = Indexes}.
