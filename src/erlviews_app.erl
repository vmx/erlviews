%% Feel free to use, reuse and abuse the code in this file.

%% @private
-module(erlviews_app).
-behaviour(application).

-record(btree, {
    fd,
    root,
    extract_kv = identity,  % fun({_Key, _Value} = KV) -> KV end,
    assemble_kv = identity, % fun({Key, Value}) -> {Key, Value} end,
    less = fun(A, B) -> A < B end,
    reduce = nil,
    kv_chunk_threshold = 16#4ff,
    kp_chunk_threshold = 2 * 16#4ff,
    binary_mode = false
}).


-include_lib("couch_set_view/include/couch_set_view.hrl").

%% API.
-export([start/2]).
-export([stop/1]).


%% API.
start(_Type, _Args) ->
    couch_file_write_guard:sup_start_link(),
    ViewBtrees = open_views("/home/vmx/src/couchbase/master/ns_server/data/n_0/data/@indexes/default/main_ac006497bacd902687106561d3900b20.view.1"),

    Dispatch = cowboy_router:compile(
                 [{'_',[
                        {"/old", erlviews_handler_old, ViewBtrees},
                        {"/new", erlviews_handler, ViewBtrees}
                       ]}
                 ]),
    {ok, _} = cowboy:start_http(http, 100, [{port, 15984}],
                                [{env, [{dispatch, Dispatch}]}]),
    erlviews_sup:start_link().

stop(_State) ->
    ok.




open_views(Filepath) ->
    {ok, Fd} = couch_file:open(Filepath),
    {ok, HeaderBin, _HeaderPos} = couch_file:read_header_bin(Fd),
    IndexHeader = couch_set_view_util:header_bin_to_term(HeaderBin),

%    KvChunkThreshold = 7168,
%    KpChunkThreshold = 6144,
%    BtreeOptions = [
%        {kv_chunk_threshold, KvChunkThreshold},
%        {kp_chunk_threshold, KpChunkThreshold},
%        {binary_mode, true}
%    ],
    ReduceFun = fun(_, _) -> ok end,
    Less = fun(A, B) ->
                   {Key1, DocId1} = decode_key_docid(A),
                   {Key2, DocId2} = decode_key_docid(B),
                   case couch_ejson_compare:less_json(Key1, Key2) of
                       0 ->
                           DocId1 < DocId2;
                       LessResult ->
                           LessResult < 0
                   end
           end,
    BtreeOptions = [
                    {less, Less},
                    {reduce, ReduceFun},
                    {binary_mode, true}
                   ],
    #set_view_index_header{
        %id_btree_state = IdBtreeState,
        view_states = ViewStates
    } = IndexHeader,

    _ViewBtrees = lists:map(
                   fun(ViewState) ->
                           {ok, Btree} = couch_btree:open(
                                           ViewState, Fd, BtreeOptions),
                           Btree
                   end, ViewStates).


-spec decode_key_docid(binary()) -> {binary(), binary()}.
decode_key_docid(<<KeyLen:16, JsonKey:KeyLen/binary, DocId/binary>>) ->
    {JsonKey, DocId}.
