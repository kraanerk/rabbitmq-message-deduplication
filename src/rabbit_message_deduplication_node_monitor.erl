%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025, Matteo Cafasso.
%% All rights reserved.

-module(rabbit_message_deduplication_node_monitor).
-behaviour(gen_event).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([start/0, stop/0]).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

-rabbit_boot_step({rabbit_message_deduplication_node_monitor,
                   [{description, "message deduplication node monitor integration"},
                    {mfa, {?MODULE, start, []}},
                    {requires, kernel_ready},
                    {enables, core_initialized}]}).

start() ->
    gen_event:add_handler(rabbit_event, ?MODULE, []).

stop() ->
    gen_event:delete_handler(rabbit_event, ?MODULE, []).

init([]) ->
    {ok, []}.

%% Handle node additions - this is called when rabbit_node_monitor emits events
handle_event(#event{type = node_added, props = Props}, State) ->
    Node = proplists:get_value(node, Props),
    case Node of
        undefined -> ok;
        _ -> 'Elixir.RabbitMQMessageDeduplication.CacheManager':on_node_up(Node)
    end,
    {ok, State};
handle_event(_Event, State) ->
    {ok, State}.

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
