%% Copyright (c) Loïc Hoguin <essen@ninenines.eu>
%%
%% Permission to use, copy, modify, and/or distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

-module(cowboy_stream_h).
-behavior(cowboy_stream).

-export([init/3]).
-export([data/4]).
-export([info/3]).
-export([terminate/3]).
-export([early_error/5]).

-export([request_process/3]).
-export([resume/5]).

-record(state, {
	next :: any(),
	ref = undefined :: ranch:ref(),
	pid = undefined :: pid(),
	expect = undefined :: undefined | continue,
	read_body_pid = undefined :: pid() | undefined,
	read_body_ref = undefined :: reference() | undefined,
	read_body_timer_ref = undefined :: reference() | undefined,
	read_body_length = 0 :: non_neg_integer() | infinity | auto,
	read_body_is_fin = nofin :: nofin | {fin, non_neg_integer()},
	read_body_buffer = <<>> :: binary(),
	body_length = 0 :: non_neg_integer(),
	stream_body_pid = undefined :: pid() | undefined,
	stream_body_status = normal :: normal | blocking | blocked
}).

-spec init(cowboy_stream:streamid(), cowboy_req:req(), cowboy:opts())
	-> {[{spawn, pid(), timeout()}], #state{}}.
init(StreamID, Req=#{ref := Ref}, Opts) ->
	Env = maps:get(env, Opts, #{}),
	Middlewares = maps:get(middlewares, Opts, [cowboy_router, cowboy_handler]),
	Shutdown = maps:get(shutdown_timeout, Opts, 5000),
	Expect = expect(Req),
	error_logger:info_msg("[cowboy_stream_h] init: StreamID=~p, Expect=~p~n", [StreamID, Expect]),
	%% Send 100-continue immediately BEFORE spawning handler to minimize delay.
	%% This is critical for boto3 which has a very short timeout (~1 second).
	{Commands100} = case Expect of
		continue ->
			error_logger:info_msg("[cowboy_stream_h] init: Sending immediate 100-continue for boto3~n"),
			{[{inform, 100, #{}}]};
		_ ->
			{[]}
	end,
	%% Now spawn the handler
	Pid = proc_lib:spawn_link(?MODULE, request_process, [Req, Env, Middlewares]),
	{Commands0, Next} = cowboy_stream:init(StreamID, Req, Opts),
	%% Combine: 100-continue first, then spawn, then other commands
	{Commands100 ++ [{spawn, Pid, Shutdown}|Commands0],
		#state{next=Next, ref=Ref, pid=Pid, expect=undefined}}.

%% Ignore the expect header in HTTP/1.0.
expect(#{version := 'HTTP/1.0'}) ->
	undefined;
expect(Req) ->
	try cowboy_req:parse_header(<<"expect">>, Req) of
		Expect ->
			Expect
	catch _:_ ->
		undefined
	end.

%% If we receive data and stream is waiting for data:
%%   If we accumulated enough data or IsFin=fin, send it.
%%   If we are in auto mode, send it and update flow control.
%%   If not, buffer it.
%% If not, buffer it.
%%
%% We always reset the expect field when we receive data,
%% since the client started sending the request body before
%% we could send a 100 continue response.

-spec data(cowboy_stream:streamid(), cowboy_stream:fin(), cowboy_req:resp_body(), State)
	-> {cowboy_stream:commands(), State} when State::#state{}.
%% Stream isn't waiting for data.
data(StreamID, IsFin, Data, State=#state{
		read_body_ref=undefined, read_body_buffer=Buffer, body_length=BodyLen}) ->
	DataFirst50 = case byte_size(Data) > 50 of
		true -> binary:part(Data, 0, 50);
		false -> Data
	end,
	error_logger:info_msg("[cowboy_stream_h] data(not_waiting): StreamID=~p, IsFin=~p, DataSize=~p, BufferSize=~p, DataFirst50=~p~n",
		[StreamID, IsFin, byte_size(Data), byte_size(Buffer), DataFirst50]),
	do_data(StreamID, IsFin, Data, [], State#state{
		expect=undefined,
		read_body_is_fin=IsFin,
		read_body_buffer= << Buffer/binary, Data/binary >>,
		body_length=BodyLen + byte_size(Data)
	});
%% Stream is waiting for data using auto mode.
%%
%% There is no buffering done in auto mode.
data(StreamID, IsFin, Data, State=#state{read_body_pid=Pid, read_body_ref=Ref,
		read_body_length=auto, body_length=BodyLen}) ->
	error_logger:info_msg("[cowboy_stream_h] data(auto_mode): StreamID=~p, IsFin=~p, DataSize=~p~n",
		[StreamID, IsFin, byte_size(Data)]),
	send_request_body(Pid, Ref, IsFin, BodyLen, Data),
	do_data(StreamID, IsFin, Data, [{flow, byte_size(Data)}], State#state{
		read_body_ref=undefined,
		body_length=BodyLen + byte_size(Data)
	});
%% Stream is waiting for data but we didn't receive enough to send yet.
data(StreamID, IsFin=nofin, Data, State=#state{
		read_body_length=ReadLen, read_body_buffer=Buffer, body_length=BodyLen})
		when byte_size(Data) + byte_size(Buffer) < ReadLen ->
	DataFirst50_ne = case byte_size(Data) > 50 of
		true -> binary:part(Data, 0, 50);
		false -> Data
	end,
	BufferFirst50_ne = case byte_size(Buffer) > 50 of
		true -> binary:part(Buffer, 0, 50);
		false -> Buffer
	end,
	error_logger:info_msg("[cowboy_stream_h] data(not_enough): StreamID=~p, DataSize=~p, BufferSize=~p, ReadLen=~p, DataFirst50=~p, BufferFirst50=~p~n",
		[StreamID, byte_size(Data), byte_size(Buffer), ReadLen, DataFirst50_ne, BufferFirst50_ne]),
	do_data(StreamID, IsFin, Data, [], State#state{
		expect=undefined,
		read_body_buffer= << Buffer/binary, Data/binary >>,
		body_length=BodyLen + byte_size(Data)
	});
%% Stream is waiting for data and we received enough to send.
data(StreamID, IsFin, Data, State=#state{read_body_pid=Pid, read_body_ref=Ref,
		read_body_timer_ref=TRef, read_body_buffer=Buffer, body_length=BodyLen0}) ->
	BodyLen = BodyLen0 + byte_size(Data),
	FullBody = <<Buffer/binary, Data/binary>>,
	BufferFirst50 = case byte_size(Buffer) > 50 of
		true -> binary:part(Buffer, 0, 50);
		false -> Buffer
	end,
	FullBodyFirst50 = case byte_size(FullBody) > 50 of
		true -> binary:part(FullBody, 0, 50);
		false -> FullBody
	end,
	error_logger:info_msg("[cowboy_stream_h] data(enough): StreamID=~p, IsFin=~p, DataSize=~p, BodyLen=~p, FullBodySize=~p, BufferFirst50=~p, FullBodyFirst50=~p~n",
		[StreamID, IsFin, byte_size(Data), BodyLen, byte_size(FullBody), BufferFirst50, FullBodyFirst50]),
	ok = erlang:cancel_timer(TRef, [{async, true}, {info, false}]),
	send_request_body(Pid, Ref, IsFin, BodyLen, FullBody),
	do_data(StreamID, IsFin, Data, [], State#state{
		expect=undefined,
		read_body_ref=undefined,
		read_body_timer_ref=undefined,
		read_body_buffer= <<>>,
		body_length=BodyLen
	}).

do_data(StreamID, IsFin, Data, Commands1, State=#state{next=Next0}) ->
	{Commands2, Next} = cowboy_stream:data(StreamID, IsFin, Data, Next0),
	{Commands1 ++ Commands2, State#state{next=Next}}.

-spec info(cowboy_stream:streamid(), any(), State)
	-> {cowboy_stream:commands(), State} when State::#state{}.
info(StreamID, Info={'EXIT', Pid, normal}, State=#state{pid=Pid}) ->
	do_info(StreamID, Info, [stop], State);
info(StreamID, Info={'EXIT', Pid, {{request_error, Reason, _HumanReadable}, _}},
		State=#state{pid=Pid}) ->
	Status = case Reason of
		timeout -> 408;
		payload_too_large -> 413;
		_ -> 400
	end,
	%% @todo Headers? Details in body? Log the crash? More stuff in debug only?
	do_info(StreamID, Info, [
		{error_response, Status, #{<<"content-length">> => <<"0">>}, <<>>},
		stop
	], State);
info(StreamID, Exit={'EXIT', Pid, Reason}, State=#state{ref=Ref, pid=Pid}) ->
	Commands0 = [{internal_error, Exit, 'Stream process crashed.'}],
	Commands = case Reason of
		normal -> Commands0;
		shutdown -> Commands0;
		{shutdown, _} -> Commands0;
		_ -> [{log, error,
				"Ranch listener ~p, connection process ~p, stream ~p "
				"had its request process ~p exit with reason ~0p~n",
				[Ref, self(), StreamID, Pid, Reason]}
			|Commands0]
	end,
	%% @todo We are trying to send a 500 response before resetting
	%%       the stream. But due to the way the RESET_STREAM frame
	%%       works in QUIC the data may be lost. The problem is
	%%       known and a draft RFC exists at
	%%       https://www.ietf.org/id/draft-ietf-quic-reliable-stream-reset-03.html
	do_info(StreamID, Exit, [
		{error_response, 500, #{<<"content-length">> => <<"0">>}, <<>>}
	|Commands], State);
%% Request body, auto mode, no body buffered.
info(StreamID, Info={read_body, Pid, Ref, auto, infinity}, State=#state{read_body_buffer= <<>>}) ->
	error_logger:info_msg("[cowboy_stream_h] info(read_body auto, no buffer): StreamID=~p~n", [StreamID]),
	do_info(StreamID, Info, [], State#state{
		read_body_pid=Pid,
		read_body_ref=Ref,
		read_body_length=auto
	});
%% Request body, auto mode, body buffered or complete.
info(StreamID, Info={read_body, Pid, Ref, auto, infinity}, State=#state{
		read_body_is_fin=IsFin, read_body_buffer=Buffer, body_length=BodyLen}) ->
	error_logger:info_msg("[cowboy_stream_h] info(read_body auto, buffered): StreamID=~p, IsFin=~p, BufferSize=~p~n",
		[StreamID, IsFin, byte_size(Buffer)]),
	send_request_body(Pid, Ref, IsFin, BodyLen, Buffer),
	do_info(StreamID, Info, [{flow, byte_size(Buffer)}],
		State#state{read_body_buffer= <<>>});
%% Request body, body buffered large enough or complete.
%%
%% We do not send a 100 continue response if the client
%% already started sending the body.
info(StreamID, Info={read_body, Pid, Ref, Length, _}, State=#state{
		read_body_is_fin=IsFin, read_body_buffer=Buffer, body_length=BodyLen})
		when IsFin =:= fin; byte_size(Buffer) >= Length ->
	BufferFirst50_eb = case byte_size(Buffer) > 50 of
		true -> binary:part(Buffer, 0, 50);
		false -> Buffer
	end,
	error_logger:info_msg("[cowboy_stream_h] info(read_body, enough buffered): StreamID=~p, IsFin=~p, BufferSize=~p, Length=~p, BufferFirst50=~p~n",
		[StreamID, IsFin, byte_size(Buffer), Length, BufferFirst50_eb]),
	send_request_body(Pid, Ref, IsFin, BodyLen, Buffer),
	do_info(StreamID, Info, [], State#state{read_body_buffer= <<>>});
%% Request body, not enough to send yet.
info(StreamID, Info={read_body, Pid, Ref, Length, Period}, State=#state{expect=Expect}) ->
	error_logger:info_msg("[cowboy_stream_h] info(read_body, not enough): StreamID=~p, Length=~p, Period=~p, Expect=~p -> sending 100-continue=~p~n",
		[StreamID, Length, Period, Expect, Expect =:= continue]),
	Commands = case Expect of
		continue -> [{inform, 100, #{}}, {flow, Length}];
		undefined -> [{flow, Length}]
	end,
	TRef = erlang:send_after(Period, self(), {{self(), StreamID}, {read_body_timeout, Ref}}),
	do_info(StreamID, Info, Commands, State#state{
		read_body_pid=Pid,
		read_body_ref=Ref,
		read_body_timer_ref=TRef,
		read_body_length=Length
	});
%% Request body reading timeout; send what we got.
info(StreamID, Info={read_body_timeout, Ref}, State=#state{read_body_pid=Pid, read_body_ref=Ref,
		read_body_is_fin=IsFin, read_body_buffer=Buffer, body_length=BodyLen}) ->
	error_logger:info_msg("[cowboy_stream_h] info(read_body_timeout): StreamID=~p, IsFin=~p, BufferSize=~p, BodyLen=~p~n",
		[StreamID, IsFin, byte_size(Buffer), BodyLen]),
	send_request_body(Pid, Ref, IsFin, BodyLen, Buffer),
	do_info(StreamID, Info, [], State#state{
		read_body_ref=undefined,
		read_body_timer_ref=undefined,
		read_body_buffer= <<>>
	});
info(StreamID, Info={read_body_timeout, _}, State) ->
	do_info(StreamID, Info, [], State);
%% Response.
%%
%% We reset the expect field when a 100 continue response
%% is sent or when any final response is sent.
info(StreamID, Inform={inform, Status, _}, State0) ->
	State = case cow_http:status_to_integer(Status) of
		100 -> State0#state{expect=undefined};
		_ -> State0
	end,
	do_info(StreamID, Inform, [Inform], State);
info(StreamID, Response={response, _, _, _}, State) ->
	do_info(StreamID, Response, [Response], State#state{expect=undefined});
info(StreamID, Headers={headers, _, _}, State) ->
	do_info(StreamID, Headers, [Headers], State#state{expect=undefined});
%% Sending data involves the data message, the stream_buffer_full alarm
%% and the connection_buffer_full alarm. We stop sending acks when an alarm is on.
%%
%% We only apply backpressure when the message includes a pid. Otherwise
%% it is a message from Cowboy, or the user circumventing the backpressure.
%%
%% We currently do not support sending data from multiple processes concurrently.
info(StreamID, Data={data, _, _}, State) ->
	do_info(StreamID, Data, [Data], State);
info(StreamID, Data0={data, Pid, _, _}, State0=#state{stream_body_status=Status}) ->
	State = case Status of
		normal ->
			Pid ! {data_ack, self()},
			State0;
		blocking ->
			State0#state{stream_body_pid=Pid, stream_body_status=blocked};
		blocked ->
			State0
	end,
	Data = erlang:delete_element(2, Data0),
	do_info(StreamID, Data, [Data], State);
info(StreamID, Alarm={alarm, Name, on}, State0=#state{stream_body_status=Status})
		when Name =:= connection_buffer_full; Name =:= stream_buffer_full ->
	State = case Status of
		normal -> State0#state{stream_body_status=blocking};
		_ -> State0
	end,
	do_info(StreamID, Alarm, [], State);
info(StreamID, Alarm={alarm, Name, off}, State=#state{stream_body_pid=Pid, stream_body_status=Status})
		when Name =:= connection_buffer_full; Name =:= stream_buffer_full ->
	_ = case Status of
		normal -> ok;
		blocking -> ok;
		blocked -> Pid ! {data_ack, self()}
	end,
	do_info(StreamID, Alarm, [], State#state{stream_body_pid=undefined, stream_body_status=normal});
info(StreamID, Trailers={trailers, _}, State) ->
	do_info(StreamID, Trailers, [Trailers], State);
info(StreamID, Push={push, _, _, _, _, _, _, _}, State) ->
	do_info(StreamID, Push, [Push], State);
info(StreamID, SwitchProtocol={switch_protocol, _, _, _}, State) ->
	do_info(StreamID, SwitchProtocol, [SwitchProtocol], State#state{expect=undefined});
%% Convert the set_options message to a command.
info(StreamID, SetOptions={set_options, _}, State) ->
	do_info(StreamID, SetOptions, [SetOptions], State);
%% Unknown message, either stray or meant for a handler down the line.
info(StreamID, Info, State) ->
	do_info(StreamID, Info, [], State).

do_info(StreamID, Info, Commands1, State0=#state{next=Next0}) ->
	{Commands2, Next} = cowboy_stream:info(StreamID, Info, Next0),
	{Commands1 ++ Commands2, State0#state{next=Next}}.

-spec terminate(cowboy_stream:streamid(), cowboy_stream:reason(), #state{}) -> ok.
terminate(StreamID, Reason, #state{next=Next}) ->
	cowboy_stream:terminate(StreamID, Reason, Next).

-spec early_error(cowboy_stream:streamid(), cowboy_stream:reason(),
	cowboy_stream:partial_req(), Resp, cowboy:opts()) -> Resp
	when Resp::cowboy_stream:resp_command().
early_error(StreamID, Reason, PartialReq, Resp, Opts) ->
	cowboy_stream:early_error(StreamID, Reason, PartialReq, Resp, Opts).

send_request_body(Pid, Ref, nofin, _, Data) ->
	error_logger:info_msg("[cowboy_stream_h] send_request_body(nofin): Pid=~p, Ref=~p, DataSize=~p~n",
		[Pid, Ref, byte_size(Data)]),
	Pid ! {request_body, Ref, nofin, Data},
	ok;
send_request_body(Pid, Ref, fin, BodyLen, Data) ->
	error_logger:info_msg("[cowboy_stream_h] send_request_body(fin): Pid=~p, Ref=~p, BodyLen=~p, DataSize=~p~n",
		[Pid, Ref, BodyLen, byte_size(Data)]),
	Pid ! {request_body, Ref, fin, BodyLen, Data},
	ok.

%% Request process.

%% We add the stacktrace to exit exceptions here in order
%% to simplify the debugging of errors. The proc_lib library
%% already adds the stacktrace to other types of exceptions.
-spec request_process(cowboy_req:req(), cowboy_middleware:env(), [module()]) -> ok.
request_process(Req, Env, Middlewares) ->
	try
		execute(Req, Env, Middlewares)
	catch
		exit:Reason={shutdown, _}:Stacktrace ->
			erlang:raise(exit, Reason, Stacktrace);
		exit:Reason:Stacktrace when Reason =/= normal, Reason =/= shutdown ->
			erlang:raise(exit, {Reason, Stacktrace}, Stacktrace)
	end.

execute(_, _, []) ->
	ok;
execute(Req, Env, [Middleware|Tail]) ->
	case Middleware:execute(Req, Env) of
		{ok, Req2, Env2} ->
			execute(Req2, Env2, Tail);
		{suspend, Module, Function, Args} ->
			proc_lib:hibernate(?MODULE, resume, [Env, Tail, Module, Function, Args]);
		{stop, _Req2} ->
			ok
	end.

-spec resume(cowboy_middleware:env(), [module()], module(), atom(), [any()]) -> ok.
resume(Env, Tail, Module, Function, Args) ->
	case apply(Module, Function, Args) of
		{ok, Req2, Env2} ->
			execute(Req2, Env2, Tail);
		{suspend, Module2, Function2, Args2} ->
			proc_lib:hibernate(?MODULE, resume, [Env, Tail, Module2, Function2, Args2]);
		{stop, _Req2} ->
			ok
	end.
