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

-type relliptics_key() :: binary().
-type relliptics_index() :: binary().
-type reliptics_index_list():: [relliptics_index()].
-type relliptics_object():: {relliptics_key(),reliptics_index_list(),term()}.

-type relliptics_connection() :: term().


open(Config) ->
    case relliptics_nif:open(Config) of
        {ok, Conn} ->
            {ok, {make_ref(), Conn}};
        Else ->
            Else
    end.

close({_Ref, Conn}) ->
    relliptics_nif:close(Conn).


-spec write(Connection::relliptics_connection(), Data::[{Object::relliptics_object(), UpdatedIndexes::reliptics_index_list()}])->any().

write({_Ref, Conn}, Data) when is_list(Data) ->
    Serialized = lists:map(fun serialize/1, Data),
    Ref = make_ref(),
    case relliptics_nif:write_async(Ref, Conn, Serialized) of
        ok ->
            receive
                {Ref, Result} ->
                    Result
            end
    end.

serialize({Object, Indexes}) ->
    {Key, CurrentIndexes, _Body} = Object,
    IndexesToSave = Indexes -- CurrentIndexes,
    IndexesToRemove = CurrentIndexes -- Indexes,
    {Key, term_to_binary(Object), IndexesToSave, IndexesToRemove}.


fold({_Ref, Conn}, Fun, Acc, Keys) when is_list(Keys)->
    Ref = make_ref(),
    case relliptics_nif:read_async(Ref, Conn, Keys) of
        ok ->
            do_fold(Ref, Fun, Acc);
        Else ->
            Else
    end.

do_fold(Ref, Fun, Acc) ->
    receive
        {Ref, {data, Object}} ->
            Acc1 = Fun(binary_to_term(Object, Acc)),
            do_fold(Ref, Fun, Acc1);
        {Ref, ok} ->
            Acc;
        Else ->
            Else
    end.


fold_indexes({_Ref, Conn}, Strategy, Fun, Acc, Indexes) when is_list(Indexes) ->
    Ref = make_ref(),
    case relliptics_nif:read_by_indexes_async(Ref, Conn, Strategy, Indexes) of
        ok ->
            do_fold(Ref, Fun, Acc);
        Else ->
            Else
    end.