%% @author Dipesh Patel <dipthegeezer.opensource@gmail.com>
%% @copyright 2012 Dipesh Patel.

%% @doc The Cassandra cql driver file, uses cqerl.

-module(erlsqlmigrate_driver_erlcass).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([create/2, up/2, down/2]).

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
bin_to_list(Arg) when is_list(Arg) ->
  Arg;
bin_to_list(Arg) when is_atom(Arg) ->
  Arg;
bin_to_list(Arg) when is_binary(Arg) ->
  binary:bin_to_list(Arg).

create([_Hostname, _Port, Keyspace] = ConnArgs, _Arg) ->
  KeyspaceName = list_to_atom(lists:concat(["keyspace_", bin_to_list(Keyspace)])),
  case connect(ConnArgs) of
    {ok, _Conn} ->
      case whereis(KeyspaceName) of
        undefined ->
          case erlcass:query(["CREATE KEYSPACE IF NOT EXISTS ", Keyspace]) of
            ok ->
              erlang:register(KeyspaceName, self());
            Res ->
              Res
          end;
        ok -> ok
      end,
      disconnect(Keyspace);
    Res -> Res
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
        false -> io:format("Skiping ~p it has not been applied~n.",
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
  Keyspace;

connect([_Hostname, _Port, Keyspace, _Username, _Password]) ->
  application:ensure_all_started(erlcass),
  Keyspace.

%% @spec transaction(Conn, Sql, Fun) -> ok
%%       Conn = pid()
%%       Sql = string()
%%       Fun = fun()
%%
%% @doc Execute a sql statement wrapped in a transaction
%% also perform execute any other function before calling commit
transaction(_Conn, Sql, Fun) ->
  % squery(Conn, "BEGIN"), % TODO Check if yugabyte supports transactions
  squery(Sql),
  Fun(),
  % squery(Conn, "COMMIT"),
  ok.

%% @spec squery(Conn, Sql) -> ok
%%       Conn = pid()
%%       Sql = string()
%%
%% @doc Execute a sql statement calling epgsql
squery(Sql) ->
  case erlcass:query(Sql) of
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
  squery(io_lib:format("INSERT INTO ~s.migrations(title, updated) VALUES(~s, Now());",
         [Conn, Title])).

%% @spec delete(Conn, Migration) -> ok
%%       Conn = pid()
%%       Migration = erlsqlmigrate_core:migration()
%%
%% @doc Delete the migrations table entry for the given migration
delete(Conn, Migration) ->
  Title = iolist_to_binary(Migration#migration.title),
  squery(io_lib:format("DELETE FROM ~s.migrations where title = ~s", [Conn, Title])).

%% @spec applied(Conn, Migration) -> ok
%%       Conn = pid()
%%       Migration = erlsqlmigrate_core:migration()
%%
%% @doc Check whether the given migration has been applied by
%% querying the migrations table
applied(Conn, Migration) ->
  Title = iolist_to_binary(Migration#migration.title),
  case squery(io_lib:format("SELECT * FROM ~s.migrations where title = ~p;",[Conn, Title])) of
    {ok, _Cols, [_Row]} -> true;
    {ok, _Cols, []} -> false
  end.

%% @spec is_setup(Conn) -> true | false
%%       Conn = pid()
%%
%% @doc Simple function to check if the migrations table is set up
%% correctly.
is_setup(Conn, Keyspace) ->
  case create(Conn, Keyspace) of
    ok -> true;
    _  -> false
  end.
