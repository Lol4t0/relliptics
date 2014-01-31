%%%-------------------------------------------------------------------
%%% @author sidorov
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. Jan 2014 10:44 AM
%%%-------------------------------------------------------------------
-module(relliptics_nif).
-author("sidorov").

%% API
-export([open/1, write_async/3, read_async/3, read_by_indexes_async/4, close/1]).
-on_load(init/0).


open(_) ->
    erlang:nif_error({error, not_loaded}).

write_async(_, _, _) ->
    erlang:nif_error({error, not_loaded}).

read_async(_, _, _) ->
    erlang:nif_error({error, not_loaded}).

read_by_indexes_async(_, _, _, _) ->
    erlang:nif_error({error, not_loaded}).

close(_) ->
    erlang:nif_error({error, not_loaded}).

init() ->
    SoName = case code:priv_dir(?MODULE) of
                 {error, bad_name} ->
                     case code:which(?MODULE) of
                         Filename when is_list(Filename) ->
                             filename:join([filename:dirname(Filename),"../priv", "relliptics"]);
                         _ ->
                             filename:join("../priv", "relliptics")
                     end;
                 Dir ->
                     filename:join(Dir, "relliptics")
             end,
    erlang:load_nif(SoName, application:get_all_env(relliptics)).