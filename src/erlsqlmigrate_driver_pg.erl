%% @author Dipesh Patel <dipthegeezer.opensource@gmail.com>
%% @copyright 2012 Dipesh Patel.

%% @doc The postgres sql driver file, uses epgsql.

-module(erlsqlmigrate_driver_pg).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([connect/1, create/2, up/2, down/2]).

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
create(ConnArgs, _Args) ->
    Conn = connect(ConnArgs),
    case is_setup(Conn) of
        true -> ok;
        false ->
            setup(Conn)
    end,
    ok = disconnect(Conn),
    ok.

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
  do_migrations(up, ConnArgs, Migrations).

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
  do_migrations(down, ConnArgs, Migrations).

do_migrations(UpDown, ConnArgs, Migrations) ->
    Conn = connect(ConnArgs),
    case is_setup(Conn) of
        true -> ok;
        false ->
          setup(Conn)
    end,
    Res = lists:foldl(
      fun (Mig, Acc) ->
        case Acc of
          ok -> do(UpDown, Conn, Mig);
          _  -> Acc
        end
      end,
      ok,
      Migrations
    ),
    ok = disconnect(Conn),
    Res.

do(up, Conn, Mig) ->
    case applied(Conn, Mig) of
        true  -> ok;
        false ->
            Fun = fun() -> update(Conn, Mig) end,
            transaction(Conn, Mig#migration.up, Fun)
    end;


do(down, Conn, Mig) ->
    case applied(Conn, Mig) of
        false ->
            lager:info("Skipping ~p it has not been applied~n.",
                            [Mig#migration.title]),
            ok;
        true  -> Fun = fun() -> delete(Conn, Mig) end,
            transaction(Conn, Mig#migration.down, Fun)
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% @spec disconnect(Conn) -> ok
%%
%% @doc Close existing connection to the postgres database
disconnect(Conn) ->
    pgsql_connection:close(Conn).

%% @spec connect(ConnArgs) -> pid()
%%       ConnArgs = list()
%%
%% @doc Connect to the postgres database using epgsql
connect([Option | _OptionsT] = Options) when is_tuple(Option) ->
    application:ensure_started(pgsql),
    case pgsql_connection:open(Options) of
        {pgsql_connection, Pid} -> {pgsql_connection, Pid};
        {error, Error}          -> {error, Error}
    end;
connect([Hostname, Port, Database, Username, Password]) ->
    application:ensure_started(pgsql),
    case pgsql_connection:open(Hostname, Username, Password,
                       [{database, Database}, {port, Port}]) of
        {pgsql_connection, Pid} -> {pgsql_connection, Pid};
        {error, Error}          -> {error, Error}
    end.

%% @spec transaction(Conn, Sql, Fun) -> ok
%%       Conn = pid()
%%       Sql = string()
%%       Fun = fun()
%%
%% @doc Execute a sql statement wrapped in a transaction
%% also perform execute any other function before calling commit
transaction_one(Conn, Sql, Fun) when is_binary(Sql)->
    squery(Conn, "BEGIN"),
    squery(Conn, Sql),
    Fun(),
    squery(Conn, "COMMIT"),
    ok.

transaction(Conn, [C | _R] = Sql, Fun) when is_integer(C)->
  try transaction_one(Conn, binary:lists_to_binary(Sql), Fun) of
    ok -> ok
  catch Error ->
    squery(Conn, "ROLLBACK"),
    {error, Error}
  end;

transaction(Conn, SqlList, Fun) when is_list(SqlList) ->
  try transaction_mult(Conn, SqlList, Fun) of
    ok -> ok
  catch Error ->
    squery(Conn, "ROLLBACK"),
    {error, Error}
  end.

transaction_mult(Conn, SqlList, Fun) ->
    squery(Conn, "BEGIN"),
    lists:foreach(
      fun(Sql) ->
        squery(Conn, Sql)
      end,
      SqlList
    ),
    Fun(),
    squery(Conn, "COMMIT"),
    ok.

%% @spec squery(Conn, Sql) -> ok
%%       Conn = pid()
%%       Sql = string()
%%
%% @doc Execute a sql statement calling epgsql
squery(Conn, Sql) ->
    lager:info(Sql),
    case pgsql_connection:simple_query(Sql, Conn) of
        {error, Error} -> throw(Error);
        Result -> Result
    end.

%% @spec equery(Conn, Sql, Params) -> ok
%%       Conn = pid()
%%       Sql = string()
%%       Params = list()
%%
%% @doc Execute a sql statement calling epgsql with parameters.
equery(Conn, Sql, Params) ->
    case pgsql_connection:extended_query(Sql, Params, Conn) of
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
    equery(Conn, "INSERT INTO migrations(title, updated) VALUES($1,now())",
           [Title]).

%% @spec delete(Conn, Migration) -> ok
%%       Conn = pid()
%%       Migration = erlsqlmigrate_core:migration()
%%
%% @doc Delete the migrations table entry for the given migration
delete(Conn, Migration) ->
    Title = iolist_to_binary(Migration#migration.title),
    equery(Conn, "DELETE FROM migrations where title = $1",
           [Title]).

%% @spec applied(Conn, Migration) -> ok
%%       Conn = pid()
%%       Migration = erlsqlmigrate_core:migration()
%%
%% @doc Check whether the given migration has been applied by
%% querying the migrations table
applied(Conn, Migration) ->
    Title = iolist_to_binary(Migration#migration.title),
    case equery(Conn, "SELECT * FROM migrations where title=$1",[Title]) of
        {{select, _Cols}, [_Row]} -> true;
        {{select, _Cols}, []} -> false
    end.

setup(Conn) ->
    try squery(Conn, "CREATE TABLE migrations(title TEXT PRIMARY KEY, updated TIMESTAMP)") of
      {{create, table}, []} -> ok
    catch Error ->
      lager:error("Error setting up migration table:~n~p~n", [Error]),
      {error, Error}
    end.

%% @spec is_setup(Conn) -> true | false
%%       Conn = pid()
%%
%% @doc Simple function to check if the migrations table is set up
%% correctly.
is_setup(Conn) ->
    case squery(Conn, "SELECT * FROM pg_tables WHERE tablename='migrations' and schemaname = current_schema()") of
        {{select, _Cols}, [_Row]} -> true;
        {{select, _Cols}, []} -> false
    end.
