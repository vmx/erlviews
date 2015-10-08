all: compile

compile:
	ERL_LIBS=/home/vmx/src/couchbase/master/install/lib/couchdb/erlang/lib ./rebar3 compile

run:
	ERL_LIBS=_build/default/lib:/home/vmx/src/couchbase/master/install/lib/couchdb/erlang/lib erl -noshell -s erlviews

clean:
	./rebar3 clean
