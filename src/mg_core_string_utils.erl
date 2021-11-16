-module(mg_core_string_utils).

-export([join/1]).
-export([join/2]).
-export([join_/1]).
-export([join_/2]).
-export([join_with/2]).
-export([mapjoin/3]).

-export([is_printable/1]).

-export_type([stringlike/0]).

-type stringlike() :: iodata() | atom() | integer().

-spec join([stringlike(), ...]) -> iodata().
join(List) ->
    join_with(List, " ").

-spec join(stringlike(), stringlike()) -> iodata().
join(S1, S2) ->
    join_with(S1, S2, " ").

-spec join_([stringlike(), ...]) -> iodata().
join_(List) ->
    join_with(List, "_").

-spec join_(stringlike(), stringlike()) -> iodata().
join_(S1, S2) ->
    join_with(S1, S2, "_").

-spec join_with([stringlike(), ...], Separator :: iodata()) -> iodata().
join_with([Elem], _) ->
    [mk_iodata(Elem)];
join_with([Elem | Rest], Sep) ->
    [mk_iodata(Elem), Sep | join_with(Rest, Sep)].

-spec join_with(stringlike(), stringlike(), Separator :: iodata()) -> iodata().
join_with(S1, S2, Sep) ->
    [mk_iodata(S1), Sep, mk_iodata(S2)].

-spec mapjoin(fun((T) -> stringlike()), [T, ...], Separator :: iodata()) -> iodata().
mapjoin(Fun, [Elem], _) ->
    [mk_iodata(Fun(Elem))];
mapjoin(Fun, [Elem | Rest], Sep) ->
    [mk_iodata(Fun(Elem)), Sep | mapjoin(Fun, Rest, Sep)].

-spec mk_iodata(stringlike()) -> iodata().
mk_iodata(B) when is_binary(B) ->
    B;
mk_iodata(L) when is_list(L) ->
    L;
mk_iodata(A) when is_atom(A) ->
    erlang:atom_to_binary(A);
mk_iodata(I) when is_integer(I) ->
    erlang:integer_to_binary(I).

-spec is_printable(binary()) -> boolean().
%% Adapted from `io_lib:printable_unicode_list/1`.
is_printable(<<C/utf8, Cs/binary>>) when C >= $\040, C =< $\176 ->
    is_printable(Cs);
is_printable(<<C/utf8, Cs/binary>>) when
    C >= 16#A0, C < 16#D800;
    C > 16#DFFF, C < 16#FFFE;
    C > 16#FFFF, C =< 16#10FFFF
->
    is_printable(Cs);
is_printable(<<>>) ->
    true;
is_printable(_) ->
    false.
