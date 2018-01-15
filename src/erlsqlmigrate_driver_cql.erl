%% @author Dipesh Patel <dipthegeezer.opensource@gmail.com>
%% @copyright 2012 Dipesh Patel.

%% @doc The Cassandra cql driver file, uses cqerl.

-module(erlsqlmigrate_driver_cql).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([create/2, up/2, down/2]).

-include("deps/cqerl/include/cqerl.hrl").
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
            Res = squery(Conn, "CREATE TABLE IF NOT EXISTS migrations(title TEXT PRIMARY KEY, updated TIMESTAMP)"),
            ok
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
    Conn = connect(ConnArgs),
    case is_setup(Conn) of
        true -> ok;
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
    ok = disconnect(Conn),
    ok.

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
    Conn = connect(ConnArgs),
    case is_setup(Conn) of
        true -> ok;
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
    ok = disconnect(Conn),
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% @spec disconnect(Conn) -> ok
%%
%% @doc Close existing connection to the postgres database
disconnect(Conn) ->
    cqerl:close_client(Conn),
    ok.

%% @spec connect(ConnArgs) -> pid()
%%       ConnArgs = list()
%%
%% @doc Connect to the Cassandra database using cqerl
connect([Hostname, Port, Database, Username, Password]) ->
    application:ensure_all_started(cqerl),
    case cqerl:get_client({Hostname, Port}, [
          {auth, {cqerl_auth_plain_handler, [{Username, Password}]}},
          {keyspace, Database}]) of
        {ok, Conn} -> Conn;
        {error, Error} -> throw(Error)
    end.

%% @spec transaction(Conn, Sql, Fun) -> ok
%%       Conn = pid()
%%       Sql = string()
%%       Fun = fun()
%%
%% @doc Execute a sql statement wrapped in a transaction
%% also perform execute any other function before calling commit
transaction(Conn, Sql, Fun) ->
    squery(Conn, "BEGIN"),
    squery(Conn, Sql),
    Fun(),
    squery(Conn, "COMMIT"),
    ok.

%% @spec squery(Conn, Sql) -> ok
%%       Conn = pid()
%%       Sql = string()
%%
%% @doc Execute a sql statement calling epgsql
squery(Conn, Sql) ->
    case cqerl:run_query(Conn, Sql) of
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
    case cqerl:run_query(Conn, #cql_query{statement=Sql, values=Params}) of
        {error, Error} -> throw(Error);
        Result -> Result
    end.

%% @spec update(Conn, Migration) -> ok
%%       Conn = pid()
%%       Migration = erlsqlmigrate_core:migration()
%%
%% @doc Insert into the migrations table the given migration.
update(Conn,Migration) ->
    Title = iolist_to_binary(Migration#migration.title),
    equery(Conn, "INSERT INTO migrations(title, updated) VALUES(? ,Now());",
           [{title, Title}]).

%% @spec delete(Conn, Migration) -> ok
%%       Conn = pid()
%%       Migration = erlsqlmigrate_core:migration()
%%
%% @doc Delete the migrations table entry for the given migration
delete(Conn,Migration) ->
    Title = iolist_to_binary(Migration#migration.title),
    equery(Conn, "DELETE FROM migrations where title = ?;",
           [{title, Title}]).

%% @spec applied(Conn, Migration) -> ok
%%       Conn = pid()
%%       Migration = erlsqlmigrate_core:migration()
%%
%% @doc Check whether the given migration has been applied by
%% querying the migrations table
applied(Conn, Migration) ->
    Title = iolist_to_binary(Migration#migration.title),
    case equery(Conn, "SELECT * FROM migrations where title = ?;",[{title, Title}]) of
        {ok, _Cols, [_Row]} -> true;
        {ok, _Cols, []} -> false
    end.

%% @spec is_setup(Conn) -> true | false
%%       Conn = pid()
%%
%% @doc Simple function to check if the migrations table is set up
%% correctly.
is_setup(Conn) ->
    case squery(Conn, "select id,keyspace_name,table_name from system_schema.tables where keyspace_name='myapp' and table_name='migrations';") of
        {ok, {cql_result, _Cols, [], _, _}} -> false;
        {ok, {cql_result, _Cols, [_Row], _, _}} -> true
    end.
