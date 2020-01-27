-compile([{parse_transform, lager_transform}]).

-module(wolfmq_task_handler).
-behaviour(wolfmq_handler).

-include("logger.hrl").

%% wolfmq_handler
-export([handle_message/1]).

%% wolfmq_handler
handle_message(Msg) ->
    case execute(Msg) of
        ok ->
            delete;
        {error, Err} ->
            ok = ?INFO("WolfMQ INSERT/UPDATE: ~p~n",[Err]),
            keep;
        {exception, Class, Reason, StackTrace}   ->
            ok = ?ERR("wolfmq exception when processing ~p. ~p:~p Stacktrace: ~p", [Msg, Class, Reason, StackTrace]),
            delete
    end.

%% internal
execute({Module, Fun, Args}) ->
    try erlang:apply(Module, Fun, Args) of
        ok -> ok;
        {ok, _} -> ok;
        Err ->
            ?INFO("WolfMQ INSERT/UPDATE: ~p~n",[Err]),
            ok
    catch
        Class:Reason -> {exception, Class, Reason, erlang:get_stacktrace()}
    end;
execute({Fun, Args}) ->
    try erlang:apply(Fun, Args) of
        ok -> ok;
        {ok, _} -> ok;
        Err -> ?INFO("WolfMQ INSERT/UPDATE: ~p~n",[Err]),
            ok
    catch
        Class:Reason -> {exception, Class, Reason, erlang:get_stacktrace()}
    end;
execute(Fun) when is_function(Fun) ->
    try Fun() of
        ok -> ok;
        {ok, _} -> ok;
        Err ->
            ?INFO("WolfMQ INSERT/UPDATE: ~p~n",[Err]),
            ok
    catch
        Class:Reason -> {exception, Class, Reason, erlang:get_stacktrace()}
    end.
