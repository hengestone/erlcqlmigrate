%% @author Dipesh Patel <dipthegeezer.opensource@gmail.com>
%% @copyright 2012 Dipesh Patel.

%% @doc The core of erlsqlmigrate, find migrations to apply.

-module(erlsqlmigrate_core).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([connect/2,
         create/3,
         create_file/3,
         up/3,
         down/3,
         list/2,
         sorted_files/2,
         run_driver/3,
         disconnect/1
        ]).

-include("migration.hrl").

%%@headerfile "migration.hrl"

%% ------------------------------------------------------------------
%% Macro Definitions
%% ------------------------------------------------------------------
-define(YAMLDIR(MigDir, Driver), filename:join([MigDir, Driver])).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
%% @spec create([{DB,ConnArgs}], MigDir, Name) -> ok
%%       DB = atom()
%%       ConnArgs = term()
%%       MigDir = filelib:dirname()
%%       Name = string()
%% @throws unknown_database
%% @doc Creates the migration files ready to be filled in with SQL.
%% Also calls the Database driver in case there is any setup needed there.
create(Config, _MigDir, []) ->
    run_driver(Config, create, []);

create([{_Driver, _ConnArgs}] = Config, MigDir, Name) ->
    create_file(Config, MigDir, Name).

create_file([{Driver, _ConnArgs}], MigDir, Name) ->
    filelib:ensure_dir(?YAMLDIR(MigDir, Driver)++"/"),
    Migration = get_migration(Driver, MigDir, Name),
    lager:debug("migration=~p~n", [Migration]),
    file:write_file(Migration#migration.yaml_path,
      "# YAML map with keys for up/down and a list of statements for each\n"
      "# Validate contents with e.g. http://www.yamllint.com/\n"
      "---\n"
      "up:\n"
      "  - CREATE TABLE example(id INTEGER);\n"
      "  - CREATE INDEX id_index ON example(id);\n"
      "down:\n"
      "  - DROP TABLE example;\n"
    ).

connect(pgsql, ConnArgs) ->
    erlsqlmigrate_driver_pg:connect(ConnArgs);
connect(mysql, ConnArgs) ->
    erlsqlmigrate_driver_my:connect(ConnArgs);
connect(erlcass, ConnArgs) ->
    erlsqlmigrate_driver_erlcass:connect(ConnArgs);
connect(_Driver, _ConnArgs) ->
    throw(unknown_database).

disconnect({pgsql_connection, _Pid} = Conn) ->
    erlsqlmigrate_driver_pg:disconnect(Conn);
disconnect({mysql_connection, _Pid} = Conn) ->
    erlsqlmigrate_driver_my:disconnect(Conn);
disconnect({erlcass_connection, _KeySpace} = Conn) ->
    erlsqlmigrate_driver_erlcass:disconnect(Conn);
disconnect(_Driver) ->
    throw(unknown_database).

%% @spec up([{DB, ConnArgs}], MigDir, Name) -> ok
%%       DB = atom()
%%       ConnArgs = term()
%%       MigDir = filelib:dirname()
%%       Name = string()
%% @throws unknown_database
%% @doc Run the up migration. Fetch migration files and pass to driver.
up([{_Driver, _ConnArgs}]=Config, MigDir, Name) ->
  do(up, Config, MigDir, Name).

%% @spec down([{DB, ConnArgs}], MigDir, Name) -> ok
%%       DB = atom()
%%       ConnArgs = term()
%%       MigDir = filelib:dirname()
%%       Name = string()
%% @doc Run the down migration. Fetch migration files and pass to driver.
down([{_Driver, _ConnArgs}]=Config, MigDir, Name) ->
  do(down, Config, MigDir, Name).

do(UpDown, [{Driver, _ConnArgs}]=Config, MigDir, Name) ->
    Files = sorted_files(UpDown, Config, MigDir, Name),
    try get_migrations(Driver, MigDir, Files) of
      Migrations ->
        case run_driver(Config, UpDown, Migrations) of
          ok -> ok;
          {error, Error} ->
            lager:error("~s migration failed:~n~p~n", [UpDown, Error]),
            {error, Error}
        end
    catch Error    ->
      lager:error("Error loading migrations:~n~p~n", [Error]),
      {error, Error}
    end.

%% @spec list([{DB, ConnArgs}], MigDir) -> {ok, [Names]}
%%       DB = atom()
%%       ConnArgs = term()
%%       MigDir = filelib:dirname()
%%       Name = string()
%% @throws unknown_database
%% @doc Run the down migration. Fetch migration files and pass to driver.
list([{_Driver, _ConnArgs}]=Config, MigDir) ->
  list(up, Config, MigDir, []).
list(UpDown, [{Driver, _ConnArgs}]=Config, MigDir, Name) ->
  Files = sorted_files(UpDown, Config, MigDir, Name),
  try get_migrations(Driver, MigDir, Files) of
    [_] = Migrations ->
      lager:info("Migrations=~p~n", [Migrations]),
      {ok, [{M#migration.title, run_driver(Config, applied, M)} || M <- Migrations]}
  catch Error    ->
    lager:error("Error loading migrations:~n~p~n", [Error]),
    {error, Error}
  end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
cmp(up, A, B) ->
   A =< B;

cmp(down, A, B) ->
   A >= B.

sorted_files([{_Driver, _ConnArgs}]=Config, MigDir) ->
  sorted_files(up, Config, MigDir, []).
sorted_files(UpDown, [{Driver, _ConnArgs}], MigDir, Name) ->
    Regex = case Name of
                [] -> "[0-9]*.yaml";
                Name -> "[0-9]*"++Name++"*.yaml"
            end,

    lists:sort(
        fun(A, B) -> cmp(UpDown, A, B) end,
        filelib:wildcard(?YAMLDIR(MigDir, Driver)++"/"++Regex)).

%% @spec run_driver([{DB,ConnArgs}],Cmd,Args) -> ok
%%       DB = atom()
%%       ConnArgs = term()
%%       Cmd = atom()
%%       Args = any()
%%
%% @throws unknown_database
%% @doc Runs command on the driver for the specified database
run_driver([{pgsql, ConnArgs}], Cmd, Args) ->
    erlsqlmigrate_driver_pg:Cmd(ConnArgs, Args);

run_driver([{mysql, ConnArgs}], Cmd, Args) ->
    erlsqlmigrate_driver_my:Cmd(ConnArgs, Args);

run_driver([{erlcass, ConnArgs}], Cmd, Args) ->
    erlsqlmigrate_driver_erlcass:Cmd(ConnArgs, Args);

run_driver([{_Dbname,_ConnArgs}], _Cmd, _Args) ->
    {error, unknown_database}.

%% @spec get_migration(Driver, MigDir, {Name, Timestamp, Up, Down}) -> migration()
%%       Driver = atom()
%%       MigDir = filelib:dirname()
%%       Name = string()
%%       TimeStamp = integer()
%%       Up = string()
%%       Down = string()
%%
%% @doc Creates a migration records from a set of parameter
get_migration(Driver, MigDir, {Name, Timestamp, Up, Down}) ->
    Title = Timestamp++"-"++Name,
    #migration{date=Timestamp,
               name=Name,
               title=Title,
               yaml_path=?YAMLDIR(MigDir, Driver)++"/"++Title++".yaml",
               up = Up,
               down = Down
              };
get_migration(Driver, MigDir, Name) ->
    Timestamp = erlsqlmigrate_utils:get_timestamp(),
    get_migration(Driver, MigDir, {Name, Timestamp, [], []}).


%% @spec get_migrations(Driver, MigDir, Files) -> [migration()] | {error, errorinfo()}
%%       Driver = atom()
%%       MigDir = filelib:dirname()
%%       Files = [file:filename()]
%%
%% @doc Creates a list of migration records from files
get_migrations(Driver, MigDir, Files) ->
    application:ensure_started(yamerl),
    lists:map(
      fun(F) ->
          case re:run(F,"\/([0-9_T:]+)\-(.*)\.yaml\$",[{capture, all_but_first, list}]) of
              {match, [Timestamp, Name]} ->
                try yamerl:decode_file(F) of
                  [[{"up", Up}, {"down", Down}]] ->
                    get_migration(Driver, MigDir, {Name, Timestamp, Up, Down});
                  OtherFormat                    ->
                    erlang:error(file_not_a_migration_file, [F, OtherFormat])
                catch
                  Error -> erlang:error(parse_error, [F, Error])
                end;
              nomatch -> erlang:error(binary:list_to_bin(io_lib:format("Invalid file name format: ~s", [F])))
          end
      end,
      Files ).
