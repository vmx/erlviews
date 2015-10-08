-module(erlviews_handler).

-export([init/2]).

-define(JSON_ENCODE(V), ejson:encode(V)).
-define(JSON_DECODE(V), ejson:decode(V)).

% From couch_db.hrl
-record(view_fold_helper_funs, {
    reduce_count,
    passed_end,
    start_response,
    send_row
}).

% From couch_db.hrl
-define(MIN_STR, <<"">>).
-define(MAX_STR, <<255>>). % illegal utf string
-record(view_query_args, {
    start_key,
    end_key,
    start_docid = ?MIN_STR,
    end_docid = ?MAX_STR,

    direction = fwd,
    inclusive_end=true, % aka a closed-interval

    limit = 10000000000, % Huge number to simplify logic
    skip = 0,

    group_level = 0,

    view_type = nil,
    include_docs = false,
    conflicts = false,
    stale = false,
    multi_get = false,
    callback = nil,
    list = nil,

    % Used by view/index merger.
    run_reduce = true,
    keys = nil,
    view_name = nil,

    debug = false,
    % Whether to filter the passive/cleanup partitions out
    filter = true,
    % Whether to query the main or the replica index
    type = main
}).


init(Req, ViewBtrees) ->
    JsonDecodeConstraint = fun(Value) -> {true, ?JSON_DECODE(Value)} end,
    #{
      start_key := StartKey,
      end_key := EndKey,
      limit := Limit
     } = cowboy_req:match_qs([
                              {start_key, JsonDecodeConstraint, undefined},
                              {end_key, JsonDecodeConstraint, undefined},
                              {limit, int, 9999999999999}],
                             Req),
    io:format("start_key: ~p, end_key: ~p, limit: ~p~n",
              [StartKey, EndKey, Limit]),
    FirstBtree = hd(ViewBtrees),

    CurrentEtag = <<"someetag">>,
    RowCount = 0,
    QueryArgs = #view_query_args{
                   start_key = StartKey,
                   end_key = EndKey
                  },
    FoldHelpers = #view_fold_helper_funs{reduce_count = nil},
    InnerFun = make_view_fold_fun(Req, QueryArgs, CurrentEtag, RowCount, FoldHelpers),

    WrapperFun = fun(KV, Reds, WrapperAcc) ->
                         ExpandedKVs = couch_set_view_util:expand_dups([KV], []),
                         fold_fun(InnerFun, ExpandedKVs, Reds, WrapperAcc)
                 end,

    SkipCount = 0,
    FoldAccInit = {Limit, SkipCount, undefined, []},
    Options = mapreduce_view:make_key_options(QueryArgs),
    {ok, _LastReduce, FoldResult} = couch_btree:fold(
                                       FirstBtree, WrapperFun, FoldAccInit,
                                       Options),
    TotalRows = 0,
    Resp = finish_view_fold(Req, TotalRows, FoldResult),

    {ok, Resp, ViewBtrees}.


fold_fun(_Fun, [], _, Acc) ->
    {ok, Acc};
fold_fun(Fun, [KV | Rest], {KVReds, Reds}, Acc) ->
    {KeyDocId, <<PartId:16, Value/binary>>} = KV,
    {JsonKey, DocId} = decode_key_docid(KeyDocId),
    case Fun({{{json, JsonKey}, DocId}, {PartId, {json, Value}}}, {KVReds, Reds}, Acc) of
    {ok, Acc2} ->
        fold_fun(Fun, Rest, {[KV | KVReds], Reds}, Acc2);
    {stop, Acc2} ->
        {stop, Acc2}
    end.


-spec decode_key_docid(binary()) -> {binary(), binary()}.
decode_key_docid(<<KeyLen:16, JsonKey:KeyLen/binary, DocId/binary>>) ->
    {JsonKey, DocId}.


% Based on couch_set_view_http
make_view_fold_fun(Req, QueryArgs, Etag, TotalViewCount, HelperFuns) ->
    #view_fold_helper_funs{
        start_response = StartRespFun,
        send_row = SendRowFun
        %reduce_count = ReduceCountFun
    } = apply_default_helper_funs(HelperFuns),

    #view_query_args{
        debug = Debug
    } = QueryArgs,

    fun(Kv, _OffsetReds, {AccLimit, AccSkip, Resp, RowFunAcc}) ->
        case {AccLimit, AccSkip, Resp} of
        {0, _, _} ->
            % we've done "limit" rows, stop foldling
            {stop, {0, 0, Resp, RowFunAcc}};
        {_, AccSkip, _} when AccSkip > 0 ->
            % just keep skipping
            {ok, {AccLimit, AccSkip - 1, Resp, RowFunAcc}};
        {_, _, undefined} ->
            % rendering the first row, first we start the response
            %Offset = ReduceCountFun(OffsetReds),
            Offset = 0,
            {ok, Resp2, RowFunAcc0} = StartRespFun(Req, Etag,
                TotalViewCount, Offset, RowFunAcc),
            {Go, RowFunAcc2} = SendRowFun(Resp2, Kv, RowFunAcc0, Debug),
            {Go, {AccLimit - 1, 0, Resp2, RowFunAcc2}};
        {AccLimit, _, Resp} when (AccLimit > 0) ->
            % rendering all other rows
            {Go, RowFunAcc2} = SendRowFun(Resp, Kv, RowFunAcc, Debug),
            {Go, {AccLimit - 1, 0, Resp, RowFunAcc2}}
        end
    end.

apply_default_helper_funs(#view_fold_helper_funs{} = Helpers) ->
    Helpers#view_fold_helper_funs{
        start_response = fun json_view_start_resp/5,
        send_row = fun send_json_view_row/4
    }.


json_view_start_resp(Req, _Etag, TotalRowCount, _Offset, _Acc) ->
    %{ok, Resp} = start_json_response(Req, 200, [{"Etag", Etag}]),
    Resp = cowboy_req:chunked_reply(200, Req),
    BeginBody = io_lib:format("{\"total_rows\":~w,", [TotalRowCount]),
    {ok, Resp, [BeginBody, "\"rows\":[\r\n"]}.

send_json_view_row(Resp, Kv, RowFront, DebugMode) ->
    JsonObj = view_row_obj(Kv, DebugMode),
    cowboy_req:chunk(RowFront ++ ?JSON_ENCODE(JsonObj), Resp),
    {ok, ",\r\n"}.


% the view row has an error
view_row_obj({{Key, error}, Value}, _DebugMode) ->
    {[{<<"key">>, Key}, {<<"error">>, Value}]};
view_row_obj({{Key, DocId}, {_PartId, Value}}, false) ->
    {[{<<"id">>, DocId}, {<<"key">>, Key}, {<<"value">>, Value}]};
view_row_obj({{Key, DocId}, {PartId, Value}}, true) ->
    {[{<<"id">>, DocId}, {<<"key">>, Key}, {<<"partition">>, PartId}, {<<"value">>, Value}]}.



finish_view_fold(Req, TotalRows, FoldResult) ->
    case FoldResult of
    {_, _, undefined, _} ->
            cowboy_req:reply(200,
                             [{<<"content-type">>, <<"application/json">>}],
                             <<"{\"total_rows\":",
                               (integer_to_binary(TotalRows))/binary,
                               "\"rows\":[]}">>,
                             Req);
    {_, _, Resp, _} ->
            % end the view
            cowboy_req:chunk(<<"\r\n]}">>, Resp),
            Resp
    end.
