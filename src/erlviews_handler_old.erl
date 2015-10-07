-module(erlviews_handler_old).

-export([init/2]).

-define(JSON_ENCODE(V), ejson:encode(V)).

%init(Req, Opts) ->
%    Req2 = cowboy_req:reply(200, [
%        {<<"content-type">>, <<"text/plain">>}
%    ], <<"Hello World!">>, Req),
%    {ok, Req2, Opts}.


init(Req, ViewBtrees) ->
    %io:format("vmx: erlviews_handler: my options: ~p~n", [Opts]),
    FirstBtree = hd(ViewBtrees),
    InnerFun = fun({{Key, DocId}, {PartId, Value}}, _Reds, {Req3, _} = Acc) when is_integer(PartId) ->
                       {json, RawValue} = Value,
                       {json, RawKey} = Key,
                       %Row = <<"{\"id\":", (?JSON_ENCODE(DocId))/binary,
                       %        ",\"key\":", (?JSON_ENCODE(Key))/binary,
                       %        ",\"value\":", RawValue/binary, "}">>,
                       Row = <<"{\"id\":", DocId/binary,
                               ",\"key\":", RawKey/binary,
                               ",\"value\":", RawValue/binary, "}">>,
                       %io:format("vmx: row is: ~p~n", [Row]),
                       cowboy_req:chunk(Row, Req3),
                       %cowboy_req:chunk("Hello\r\n", Req3),
                       {ok, Acc}
               end,
    WrapperFun = fun(KV, Reds, Acc2) ->
                         ExpandedKVs = couch_set_view_util:expand_dups([KV], []),
                         fold_fun(InnerFun, ExpandedKVs, Reds, Acc2),
                         case Acc2 of
                             {_, 0} ->
                                 {stop, 0};
                             {Req3, Count} ->
                                 {ok, {Req3, Count - 1}}
                         end
                 end,

    Req2 = cowboy_req:chunked_reply(200, Req),

    Acc = {Req2, 10},
    Options = [],
    couch_btree:fold(FirstBtree, WrapperFun, Acc, Options),

%    Req2 = cowboy_req:chunked_reply(200, Req),
%    cowboy_req:chunk("Hello\r\n", Req2),
%    %timer:sleep(1000),
%    cowboy_req:chunk("World\r\n", Req2),
%    %timer:sleep(1000),
%    cowboy_req:chunk("Chunked!\r\n", Req2),
    {ok, Req2, ViewBtrees}.


fold_fun(_Fun, [], _, Acc) ->
    {ok, Acc};
fold_fun(Fun, [KV | Rest], {KVReds, Reds}, Acc) ->
    {KeyDocId, <<PartId:16, Value/binary>>} = KV,
    {JsonKey, DocId} = decode_key_docid(KeyDocId),
    %case Fun({{{json, JsonKey}, DocId}, {PartId, {json, Value}}}, {KVReds, Reds}, Acc) of
    case Fun({{{json, JsonKey}, DocId}, {PartId, {json, Value}}}, {KVReds, Reds}, Acc) of
    {ok, Acc2} ->
        fold_fun(Fun, Rest, {[KV | KVReds], Reds}, Acc2);
    {stop, Acc2} ->
        {stop, Acc2}
    end.


-spec decode_key_docid(binary()) -> {binary(), binary()}.
decode_key_docid(<<KeyLen:16, JsonKey:KeyLen/binary, DocId/binary>>) ->
    {JsonKey, DocId}.



%Req2 = cowboy_req:chunked_reply(200, [
%    {<<"content-type">>, <<"text/html">>}
%], Req),
%cowboy_req:chunk("<html><head>Hello world!</head>", Req2),
%cowboy_req:chunk("<body><p>Hats off!</p></body></html>", Req2).



