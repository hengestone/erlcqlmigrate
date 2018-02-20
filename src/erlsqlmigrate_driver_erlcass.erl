%% @author Dipesh Patel <dipthegeezer.opensource@gmail.com>
%% @copyright 2012 Dipesh Patel.

%% @doc The Cassandra cql driver file, uses cqerl.

-module(erlsqlmigrate_driver_erlcass).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([create/2, up/2, down/2, has_keyspace/1]).

-include("migration.hrl").

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

%% @spec create(Config, Args) -> ok
%%       Config = erlsqlmigrate:config()
%%       Args = [any()]
%%
%%
%% @doc Create the migration table in the Database if it doesn't
%% already exist

create_ets(Keyspace) ->
  case ets:info(erlcass) of
    undefined ->
      ets:new(erlcass, [set, protected, named_table]),
      ets:insert(erlcass, {Keyspace, self()});
    _ ->
      true
  end.

has_keyspace(Keyspace) ->
  case ets:info(erlcass) of
    undefined -> false;
    _         ->
      case ets:lookup(erlcass, Keyspace) of
        [{Keyspace, _}] -> true;
        []              -> false
      end
  end.

create([Hostname, Port, Keyspace, _User, _Password], Arg) ->
  create([Hostname, Port, Keyspace], Arg);
create([_Hostname, _Port, Keyspace] = ConnArgs, _Arg) ->
  case connect(ConnArgs) of
    {ok, _Conn} ->
      create(Keyspace, []),
      disconnect(Keyspace);
    Res ->
      io:format("Connect returned ~p~n", [Res]),
      Res
  end;

create(Keyspace, _Arg) ->
  case has_keyspace(Keyspace) of
    false ->
      case squery(Keyspace, "CREATE KEYSPACE IF NOT EXISTS {{keyspace}};") of
        ok ->
          create_ets(Keyspace),
          ok;
        Res ->
          Res
      end,
      squery(Keyspace, "CREATE TABLE IF NOT EXISTS {{keyspace}}.migrations(title TEXT PRIMARY KEY,updated TIMESTAMP);");
    Res ->
      ok
  end.



%% @spec up(Config, Migrations) -> ok
%%       Config = erlsqlmigrate:config()
%%       Migrations = [erlsqlmigrate_core:migration()]
%%
%% @throws setup_error
%%
%% @doc Execute the migrations in the given list of migrations.
%% Each execution will be wrapped in a transaction and the
%% migrations table will be updated to indicate it has been
%% executed. Note if the migration has already been applied
%% it will be skipped
up(ConnArgs, Migrations) ->
  {ok, Conn} = connect(ConnArgs),
  case is_setup(Conn, []) of
      true  -> ok;
      false -> throw(setup_error)
  end,
  lists:foreach(
    fun(Mig) ->
      case applied(Conn, Mig) of
          true  -> ok;
          false -> Fun = fun() -> update(Conn, Mig) end,
                   transaction(Conn, Mig#migration.up, Fun)
      end
    end, Migrations),
  disconnect(Conn).

%% @spec down(Config, Migrations) -> ok
%%       Config = erlsqlmigrate:config()
%%       Migrations = [erlsqlmigrate_core:migration()]
%%
%% @throws setup_error
%%
%% @doc Execute the down migrations in the given list of migrations.
%% Each execution will be wrapped in a transaction and the migrations
%% table will have the migration entry removed to indicate it has
%% been executed. Note if the up migration has not been applied it
%% will be skipped.
down(ConnArgs, Migrations) ->
  {ok, Conn} = connect(ConnArgs),
  case is_setup(Conn, []) of
      true  -> ok;
      false -> throw(setup_error)
  end,

  lists:foreach(
    fun(Mig) ->
      case applied(Conn, Mig) of
        false -> io:format("Skipping ~p it has not been applied~n.",
                           [Mig#migration.title]),
                 ok;
        true -> Fun = fun() -> delete(Conn,Mig) end,
                transaction(Conn, Mig#migration.down, Fun)
      end
    end, Migrations),
  disconnect(Conn).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% @spec disconnect(Conn) -> ok
%%
%% @doc Close existing connection to the cql database
disconnect(_Conn) ->
  ok.

%% @spec connect(ConnArgs) -> pid()
%%       ConnArgs = list()
%%
%% @doc Connect to the Cassandra database using erlcass
connect([_Hostname, _Port, Keyspace]) ->
  application:ensure_all_started(erlcass),
  {ok, Keyspace};

connect([_Hostname, _Port, Keyspace, _Username, _Password]) ->
  application:ensure_all_started(erlcass),
  {ok, Keyspace}.

%% @spec transaction(Conn, Sql, Fun) -> ok
%%       Conn = pid()
%%       Sql = string()
%%       Fun = fun()
%%
%% @doc Execute a sql statement wrapped in a transaction
%% also perform execute any other function before calling commit
transaction(Conn, Sql, Fun) ->
  % squery(Conn, "BEGIN"), % TODO Check if yugabyte supports transactions
  squery(Conn, Sql),
  Fun(),
  % squery(Conn, "COMMIT"),
  ok.

%% @spec squery(Conn, Sql) -> ok
%%       Conn = string()
%%       Sql = string()
%%
%% @doc Execute a sql statement calling epgsql
squery(Conn, Sql) when is_list(Sql)->
  squery(Conn, binary:list_to_bin(Sql));

squery(Conn, Sql) when is_binary(Sql) ->
  Ctx = [{"keyspace", Conn}, {"now", erlang:system_time(microsecond)}],
  Query = bbmustache:render(Sql, Ctx),
  case erlcass:query(Query) of
      {error, Error} -> throw(Error);
      Result -> Result
  end.

%% @spec update(Conn, Migration) -> ok
%%       Conn = pid()
%%       Migration = erlsqlmigrate_core:migration()
%%
%% @doc Insert into the migrations table the given migration.
update(Conn, Migration) ->
  Title = iolist_to_binary(Migration#migration.title),
  squery(Conn, io_lib:format("INSERT INTO ~s.migrations(title, updated) VALUES('~s', ~w);",
         [Conn, Title, erlang:system_time(microsecond)])).

%% @spec delete(Conn, Migration) -> ok
%%       Conn = pid()
%%       Migration = erlsqlmigrate_core:migration()
%%
%% @doc Delete the migrations table entry for the given migration
delete(Conn, Migration) ->
  Title = iolist_to_binary(Migration#migration.title),
  squery(Conn, io_lib:format("DELETE FROM ~s.migrations where title = '~s';", [Conn, Title])).

%% @spec applied(Conn, Migration) -> ok
%%       Conn = pid()
%%       Migration = erlsqlmigrate_core:migration()
%%
%% @doc Check whether the given migration has been applied by
%% querying the migrations table
applied(Conn, Migration) ->
  Title = iolist_to_binary(Migration#migration.title),
  case squery(Conn, io_lib:format("SELECT * FROM ~s.migrations where title = '~s';",[Conn, Title])) of
    {ok, _Cols, [_Row]} -> true;
    {ok, _Cols, []} -> false
  end.

%% @spec is_setup(Conn) -> true | false
%%       Conn = pid()
%%
%% @doc Simple function to check if the migrations table is set up
%% correctly.
is_setup(Keyspace, _Arg) ->
  case create(Keyspace, []) of
    ok -> true;
    _  -> false
  end.
