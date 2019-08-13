%% -*- coding:utf-8;Mode:erlang;tab-width:4;c-basic-offset:4;indent-tabs-mode:nil -*-
%% ex: set softtabstop=4 tabstop=4 shiftwidth=4 expandtab fileencoding=utf-8:
%%
%% Copyright (c) 2011 Yurii Rashkovskii, Evax Software and Michael Truog
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
%% THE SOFTWARE.

-module(erlzmq).
%% @headerfile "erlzmq.hrl"
-include_lib("erlzmq.hrl").
-export([context/0,
         context/1,
         socket/2,
         bind/2,
         connect/2,
         send/2,
         send/3,
         sendmsg/2,
         sendmsg/3,
         recv/1,
         recv/2,
         recvmsg/1,
         recvmsg/2,
         setsockopt/3,
         getsockopt/2,
         close/1,
         close/2,
         term/1,
         term/2,
         version/0]).
-export_type([erlzmq_socket/0, erlzmq_context/0]).

%% @equiv context(1)
-spec context() ->
    {ok, erlzmq_context()} |
    erlzmq_error().
context() ->
    context(1).

%% @doc Create a new erlzmq context with the specified number of io threads.
%% <br />
%% If the context can be created an 'ok' tuple containing an
%% {@type erlzmq_context()} handle to the created context is returned;
%% if not, it returns an 'error' tuple with an {@type erlzmq_type_error()}
%% describing the error.
%% <br />
%% The context must be later cleaned up calling {@link erlzmq:term/1. term/1}
%% <br />
%% <i>For more information see
%% <a href="http://api.zeromq.org/master:zmq-init">zmq_init</a></i>
%% @end
-spec context(Threads :: pos_integer()) ->
    {ok, erlzmq_context()} |
    erlzmq_error().
context(Threads) when is_integer(Threads) ->
    erlzmq_nif:context(Threads).


%% @doc Create a socket.
%% <br />
%% This functions creates a socket of the given
%% {@link erlzmq_socket_type(). type}, optionally setting it to active mode,
%% and associates it with the given {@link erlzmq_context(). context}.
%% <br />
%% If the socket can be created an 'ok' tuple containing a
%% {@type erlzmq_socket()} handle to the created socket is returned;
%% if not, it returns an {@type erlzmq_error()} describing the error.
%% <br />
%% In line with Erlang's socket paradigm,  a socket can be either active or
%% passive. Passive sockets tend to have lower latency and have a higher
%% throughput for small message sizes. Active sockets on the contrary give
%% the highest throughput for messages above 32k. A benchmarking tool is
%% included in the source distribution.<br />
%% <i>For more information see
%% <a href="http://api.zeromq.org/master:zmq_socket">zmq_socket</a>.</i>
%% @end
-spec socket(Context :: erlzmq_context(),
             Type :: erlzmq_socket_type() |
                     list(erlzmq_socket_type() |
                          {active, boolean()} |
                          {active_pid, pid()})) ->
                    {ok, erlzmq_socket()} |
                    erlzmq_error().
socket(Context, Type) when is_atom(Type) ->
    socket(Context, [Type]);
socket(Context, [H | T]) when is_atom(H) ->
    case T of
        [] ->
            % active is false by default
            % (to avoid latency on small messages (messages < 32KB))
            socket(Context, H, {active, false});
        [Active] ->
            socket(Context, H, Active)
    end;
socket(Context, [H | [Type]]) when is_tuple(H) ->
    socket(Context, Type, H).

-spec socket(Context :: erlzmq_context(),
             Type :: erlzmq_socket_type(),
             {active, boolean()} | {active_pid, pid()}) ->
                    {ok, erlzmq_socket()} |
                    erlzmq_error().
socket(Context, Type, {active, true}) ->
    true = (Type =/= pub) and (Type =/= push) and (Type =/= xpub),
    erlzmq_nif:socket(Context, socket_type(Type), 1, self());
socket(Context, Type, {active_pid, Pid})
    when is_pid(Pid), node(Pid) =:= node() ->
    true = (Type =/= pub) and (Type =/= push) and (Type =/= xpub),
    erlzmq_nif:socket(Context, socket_type(Type), 1, Pid);
socket(Context, Type, {active, false}) ->
    erlzmq_nif:socket(Context, socket_type(Type), 0, self()).


%% @doc Accept connections on a socket.
%% <br />
%% <i>For more information see
%% <a href="http://api.zeromq.org/master:zmq_bind">zmq_bind</a>.</i>
%% @end
-spec bind(Socket :: erlzmq_socket(),
           Endpoint :: erlzmq_endpoint()) ->
    ok |
    erlzmq_error().
bind({I, Socket}, Endpoint)
    when is_integer(I), is_list(Endpoint) ->
    erlzmq_nif:bind(Socket, Endpoint);
bind({I, Socket}, Endpoint)
    when is_integer(I), is_binary(Endpoint) ->
    bind({I, Socket}, binary_to_list(Endpoint)).

%% @doc Connect a socket.
%% <br />
%% <i>For more information see
%% <a href="http://api.zeromq.org/master:zmq_connect">zmq_connect</a>.</i>
%% @end
-spec connect(Socket :: erlzmq_socket(),
              Endpoint :: erlzmq_endpoint()) ->
    ok |
    erlzmq_error().
connect({I, Socket}, Endpoint)
    when is_integer(I), is_list(Endpoint) ->
    erlzmq_nif:connect(Socket, Endpoint);
connect({I, Socket}, Endpoint)
    when is_integer(I), is_binary(Endpoint) ->
    connect({I, Socket}, binary_to_list(Endpoint)).

%% @equiv send(Socket, Msg, [])
-spec send(erlzmq_socket(),
           Binary :: binary()) ->
    ok |
    erlzmq_error().
send(Socket, Binary) when is_binary(Binary) ->
    send(Socket, Binary, []).

%% @doc Send a message on a socket.
%% <br />
%% <i>For more information see
%% <a href="http://api.zeromq.org/master:zmq_send">zmq_send</a>.</i>
%% @end
-spec send(erlzmq_socket(),
           Binary :: binary(),
           Flags :: erlzmq_send_recv_flags()) ->
    ok |
    erlzmq_error().
send({I, Socket}, Binary, Flags)
    when is_integer(I), is_binary(Binary), is_list(Flags) ->
    case erlzmq_nif:send(Socket, Binary, sendrecv_flags(Flags)) of
        Ref when is_reference(Ref) ->
            receive
                {Ref, ok} ->
                    ok;
                {Ref, {error, _} = Error} ->
                    Error
            after case  erlzmq_nif:getsockopt(Socket,?'ZMQ_SNDTIMEO') of
                       {ok, -1} ->
                           infinity;
                       {ok, Else} ->
                           Else
                   end ->
                    {error, eagain}
            end;
        Result ->
            Result
    end.

%% @equiv send(Socket, Msg, [])
%% @doc This function exists for zeromq api compatibility and doesn't
%% actually provide any different functionality then what you get with
%% the {@link erlzmq:send/2} function. In fact this function just
%% calls that function. So there is a slight bit of additional
%% overhead as well.
-spec sendmsg(erlzmq_socket(),
           Binary :: binary()) ->
    ok |
    erlzmq_error().
sendmsg(Socket, Binary) when is_binary(Binary) ->
    send(Socket, Binary, []).

%% @equiv send(Socket, Msg, Flags)
%% @doc This function exists for zeromq api compatibility and doesn't
%% actually provide any different functionality then what you get with
%% the {@link erlzmq:send/3} function. In fact this function just
%% calls that function. So there is a slight bit of additional
%% overhead as well.
-spec sendmsg(erlzmq_socket(),
           Binary :: binary(),
           Flags :: erlzmq_send_recv_flags()) ->
    ok |
    erlzmq_error().
sendmsg(Socket, Binary, Flags) ->
    send(Socket, Binary, Flags).


%% @equiv recv(Socket, 0)
-spec recv(Socket :: erlzmq_socket()) ->
    {ok, erlzmq_data()} |
    erlzmq_error().
recv(Socket) ->
    recv(Socket, []).

%% @doc Receive a message from a socket.
%% <br />
%% <i>For more information see
%% <a href="http://api.zeromq.org/master:zmq_recv">zmq_recv</a>.</i>
%% @end
-spec recv(Socket :: erlzmq_socket(),
           Flags :: erlzmq_send_recv_flags()) ->
    {ok, erlzmq_data()} |
    erlzmq_error().
recv({I, Socket}, Flags)
    when is_integer(I), is_list(Flags) ->
    case erlzmq_nif:recv(Socket, sendrecv_flags(Flags)) of
        Ref when is_reference(Ref) ->
            receive
                {Ref, {error, _} = Error} ->
                    Error;
                {Ref, Result} ->
                    {ok, Result}
            after case erlzmq_nif:getsockopt(Socket,?'ZMQ_RCVTIMEO') of
                      {ok, -1} ->
                          infinity;
                      {ok, Else} ->
                          Else
                  end ->
                    {error, eagain}
            end;
        Result ->
            Result
    end.

%% @equiv recv(Socket, 0)
%% @doc This function exists for zeromq api compatibility and doesn't
%% actually provide any different functionality then what you get with
%% the {@link erlzmq:recv/3} function. In fact this function just
%% calls that function. So there is a slight bit of additional
%% overhead as well.
-spec recvmsg(Socket :: erlzmq_socket()) ->
    {ok, erlzmq_data()} |
    erlzmq_error().
recvmsg(Socket) ->
    recv(Socket, []).

%% @equiv recv(Socket, Flags)
%% @doc This function exists for zeromq api compatibility and doesn't
%% actually provide any different functionality then what you get with
%% the {@link erlzmq:recv/3} function. In fact this function just
%% calls that function. So there is a slight bit of additional
%% overhead as well.
-spec recvmsg(Socket :: erlzmq_socket(),
           Flags :: erlzmq_send_recv_flags()) ->
    {ok, erlzmq_data()} |
    erlzmq_error().
recvmsg(Socket, Flags) ->
    recv(Socket, Flags).

%% @doc Set an {@link erlzmq_sockopt(). option} associated with a socket.
%% <br />
%% <i>For more information see
%% <a href="http://api.zeromq.org/master:zmq_setsockopt">zmq_setsockopt</a>.</i>
%% @end
-spec setsockopt(erlzmq_socket(),
                 Name :: erlzmq_sockopt(),
                 erlzmq_sockopt_value() | binary()) ->
    ok |
    erlzmq_error().
setsockopt(Socket, Name, Value) when is_list(Value) ->
    setsockopt(Socket, Name, erlang:list_to_binary(Value));
setsockopt({I, Socket}, Name, Value) when is_integer(I), is_atom(Name) ->
    erlzmq_nif:setsockopt(Socket, option_name(Name), Value).

%% @doc Get an {@link erlzmq_sockopt(). option} associated with a socket.
%% <br />
%% <i>For more information see
%% <a href="http://api.zeromq.org/master:zmq_getsockopt">zmq_getsockopt</a>.</i>
%% @end
-spec getsockopt(Socket :: erlzmq_socket(),
                 Name :: erlzmq_sockopt()) ->
    {ok, erlzmq_sockopt_value()} |
    erlzmq_error().
getsockopt({I, Socket}, Name) when is_integer(I), is_atom(Name) ->
    erlzmq_nif:getsockopt(Socket, option_name(Name)).

%% @equiv close(Socket, infinity)
-spec close(Socket :: erlzmq_socket()) ->
    ok |
    erlzmq_error().
close(Socket) ->
    close(Socket, infinity).

%% @doc Close the given socket.
%% <br />
%% <i>For more information see
%% <a href="http://api.zeromq.org/master:zmq_close">zmq_close</a>.</i>
%% @end
-spec close(Socket :: erlzmq_socket(),
            Timeout :: timeout()) ->
    ok |
    erlzmq_error().
close({I, Socket}, Timeout) when is_integer(I) ->
    case erlzmq_nif:close(Socket) of
        Ref when is_reference(Ref) ->
            receive
                {Ref, Result} ->
                    Result
            after
                Timeout ->
                    {error, {timeout, Ref}}
            end;
        Result ->
            Result
    end.

%% @equiv term(Context, infinity)
-spec term(Context :: erlzmq_context()) ->
    ok |
    erlzmq_error().
term(Context) ->
    term(Context, infinity).

%% @doc Terminate the given context waiting up to Timeout ms.
%% <br />
%% This function should be called after all sockets associated with
%% the given context have been closed.<br />
%% If not it will block the given Timeout amount of time.
%% <i>For more information see
%% <a href="http://api.zeromq.org/master:zmq_term">zmq_term</a>.</i>
%% @end
-spec term(Context :: erlzmq_context(),
           Timeout :: timeout()) ->
    ok |
    erlzmq_error() |
    {error, {timeout, reference()}}.

term(Context, Timeout) ->
    case erlzmq_nif:term(Context) of
        Ref when is_reference(Ref) ->
            receive
                {Ref, Result} ->
                    Result
            after
                Timeout ->
                    {error, {timeout, Ref}}
            end;
        Result ->
            Result
    end.

%% @doc Returns the 0MQ library version.
%% @end
-spec version() -> {integer(), integer(), integer()}.

version() -> erlzmq_nif:version().

%% Private

-spec socket_type(Type :: erlzmq_socket_type()) ->
    integer().

socket_type(pair) ->
    ?'ZMQ_PAIR';
socket_type(pub) ->
    ?'ZMQ_PUB';
socket_type(sub) ->
    ?'ZMQ_SUB';
socket_type(req) ->
    ?'ZMQ_REQ';
socket_type(rep) ->
    ?'ZMQ_REP';
socket_type(dealer) ->
    ?'ZMQ_DEALER';
% deprecated
socket_type(xreq) ->
    ?'ZMQ_XREQ';
socket_type(router) ->
    ?'ZMQ_ROUTER';
% deprecated
socket_type(xrep) ->
    ?'ZMQ_XREP';
socket_type(pull) ->
    ?'ZMQ_PULL';
socket_type(push) ->
    ?'ZMQ_PUSH';
socket_type(xpub) ->
    ?'ZMQ_XPUB';
socket_type(xsub) ->
    ?'ZMQ_XSUB';
socket_type(stream) ->
    ?'ZMQ_STREAM'.

-spec sendrecv_flags(Flags :: erlzmq_send_recv_flags()) ->
    integer().

sendrecv_flags([]) ->
    0;
sendrecv_flags([dontwait|Rest]) ->
    ?'ZMQ_DONTWAIT' bor sendrecv_flags(Rest);
sendrecv_flags([sndmore|Rest]) ->
    ?'ZMQ_SNDMORE' bor sendrecv_flags(Rest).

-spec option_name(Name :: erlzmq_sockopt()) ->
    integer().

option_name(affinity) -> ?'ZMQ_AFFINITY';
option_name(routing_id) -> ?'ZMQ_ROUTING_ID';
option_name(subscribe) -> ?'ZMQ_SUBSCRIBE';
option_name(unsubscribe) -> ?'ZMQ_UNSUBSCRIBE';
option_name(rate) -> ?'ZMQ_RATE';
option_name(recovery_ivl) -> ?'ZMQ_RECOVERY_IVL';
option_name(sndbuf) -> ?'ZMQ_SNDBUF';
option_name(rcvbuf) -> ?'ZMQ_RCVBUF';
option_name(rcvmore) -> ?'ZMQ_RCVMORE';
option_name(fd) -> ?'ZMQ_FD';
option_name(events) -> ?'ZMQ_EVENTS';
option_name(type) -> ?'ZMQ_TYPE';
option_name(linger) -> ?'ZMQ_LINGER';
option_name(reconnect_ivl) -> ?'ZMQ_RECONNECT_IVL';
option_name(backlog) -> ?'ZMQ_BACKLOG';
option_name(reconnect_ivl_max) -> ?'ZMQ_RECONNECT_IVL_MAX';
option_name(maxmsgsize) -> ?'ZMQ_MAXMSGSIZE';
option_name(sndhwm) -> ?'ZMQ_SNDHWM';
option_name(rcvhwm) -> ?'ZMQ_RCVHWM';
option_name(multicast_hops) -> ?'ZMQ_MULTICAST_HOPS';
option_name(rcvtimeo) -> ?'ZMQ_RCVTIMEO';
option_name(sndtimeo) -> ?'ZMQ_SNDTIMEO';
option_name(last_endpoint) -> ?'ZMQ_LAST_ENDPOINT';
option_name(router_mandatory) -> ?'ZMQ_ROUTER_MANDATORY';
option_name(tcp_keepalive) -> ?'ZMQ_TCP_KEEPALIVE';
option_name(tcp_keepalive_cnt) -> ?'ZMQ_TCP_KEEPALIVE_CNT';
option_name(tcp_keepalive_idle) -> ?'ZMQ_TCP_KEEPALIVE_IDLE';
option_name(tcp_keepalive_intvl) -> ?'ZMQ_TCP_KEEPALIVE_INTVL';
option_name(immediate) -> ?'ZMQ_IMMEDIATE';
option_name(xpub_verbose) -> ?'ZMQ_XPUB_VERBOSE';
option_name(router_raw) -> ?'ZMQ_ROUTER_RAW';
option_name(ipv6) -> ?'ZMQ_IPV6';
option_name(mechanism) -> ?'ZMQ_MECHANISM';
option_name(plain_server) -> ?'ZMQ_PLAIN_SERVER';
option_name(plain_username) -> ?'ZMQ_PLAIN_USERNAME';
option_name(plain_password) -> ?'ZMQ_PLAIN_PASSWORD';
option_name(curve_server) -> ?'ZMQ_CURVE_SERVER';
option_name(curve_publickey) -> ?'ZMQ_CURVE_PUBLICKEY';
option_name(curve_secretkey) -> ?'ZMQ_CURVE_SECRETKEY';
option_name(curve_serverkey) -> ?'ZMQ_CURVE_SERVERKEY';
option_name(probe_router) -> ?'ZMQ_PROBE_ROUTER';
option_name(req_correlate) -> ?'ZMQ_REQ_CORRELATE';
option_name(req_relaxed) -> ?'ZMQ_REQ_RELAXED';
option_name(conflate) -> ?'ZMQ_CONFLATE';
option_name(zap_domain) -> ?'ZMQ_ZAP_DOMAIN';
option_name(router_handover) -> ?'ZMQ_ROUTER_HANDOVER';
option_name(tos) -> ?'ZMQ_TOS';
option_name(connect_routing_id) -> ?'ZMQ_CONNECT_ROUTING_ID';
option_name(gssapi_server) -> ?'ZMQ_GSSAPI_SERVER';
option_name(gssapi_principal) -> ?'ZMQ_GSSAPI_PRINCIPAL';
option_name(gssapi_service_principal) -> ?'ZMQ_GSSAPI_SERVICE_PRINCIPAL';
option_name(gssapi_plaintext) -> ?'ZMQ_GSSAPI_PLAINTEXT';
option_name(handshake_ivl) -> ?'ZMQ_HANDSHAKE_IVL';
option_name(socks_proxy) -> ?'ZMQ_SOCKS_PROXY';
option_name(xpub_nodrop) -> ?'ZMQ_XPUB_NODROP';
% context option
% option_name(blocky) -> ?'ZMQ_BLOCKY';
option_name(xpub_manual) -> ?'ZMQ_XPUB_MANUAL';
option_name(xpub_welcome_msg) -> ?'ZMQ_XPUB_WELCOME_MSG';
option_name(stream_notify) -> ?'ZMQ_STREAM_NOTIFY';
option_name(invert_matching) -> ?'ZMQ_INVERT_MATCHING';
option_name(heartbeat_ivl) -> ?'ZMQ_HEARTBEAT_IVL';
option_name(heartbeat_ttl) -> ?'ZMQ_HEARTBEAT_TTL';
option_name(heartbeat_timeout) -> ?'ZMQ_HEARTBEAT_TIMEOUT';
option_name(xpub_verboser) -> ?'ZMQ_XPUB_VERBOSER';
option_name(connect_timeout) -> ?'ZMQ_CONNECT_TIMEOUT';
option_name(tcp_maxrt) -> ?'ZMQ_TCP_MAXRT';
option_name(thread_safe) -> ?'ZMQ_THREAD_SAFE';
option_name(multicast_maxtpdu) -> ?'ZMQ_MULTICAST_MAXTPDU';
option_name(vmci_buffer_size) -> ?'ZMQ_VMCI_BUFFER_SIZE';
option_name(vmci_buffer_min_size) -> ?'ZMQ_VMCI_BUFFER_MIN_SIZE';
option_name(vmci_buffer_max_size) -> ?'ZMQ_VMCI_BUFFER_MAX_SIZE';
option_name(vmci_connect_timeout) -> ?'ZMQ_VMCI_CONNECT_TIMEOUT';
option_name(use_fd) -> ?'ZMQ_USE_FD';
option_name(gssapi_principal_nametype) -> ?'ZMQ_GSSAPI_PRINCIPAL_NAMETYPE';
option_name(gssapi_service_principal_nametype) -> ?'ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE';
option_name(bindtodevice) -> ?'ZMQ_BINDTODEVICE';
% deprecated
option_name(ipv4only) -> ?'ZMQ_IPV4ONLY';
option_name(tcp_accept_filter) -> ?'ZMQ_TCP_ACCEPT_FILTER';
option_name(connect_rid) -> ?'ZMQ_CONNECT_RID';
option_name(delay_attach_on_connect) -> ?'ZMQ_DELAY_ATTACH_ON_CONNECT';
option_name(noblock) -> ?'ZMQ_NOBLOCK';
option_name(fail_unroutable) -> ?'ZMQ_FAIL_UNROUTABLE';
option_name(router_behavior) -> ?'ZMQ_ROUTER_BEHAVIOR';
option_name(identity) -> ?'ZMQ_IDENTITY'.
