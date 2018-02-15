-module(erlsqlmigrate_utils).
-export([get_timestamp/0]).

%% @spec () -> integer()
%% @doc Return the current timestamp via erlang:now().
get_timestamp() ->
  {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:universal_time(),
  lists:flatten(io_lib:format("~4..0w~2..0w~2..0w:~2..0w~2..0w~2..0w",[Year,Month,Day,Hour,Minute,Second])).
