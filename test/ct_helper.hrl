-ifndef(__mg_core_ct_helper__).
-define(__mg_core_ct_helper__, 42).

-define(flushMailbox(),
    (fun __Flush() ->
        receive
            __M -> [__M | __Flush()]
        after 0 -> []
        end
    end)()
).

-define(assertReceive(__Expr),
    ?assertReceive(__Expr, 1000)
).

-define(assertReceive(__Expr, __Timeout),
    (fun() ->
        receive
            (__Expr) = __V -> __V
        after __Timeout ->
            erlang:error(
                {assertReceive, [
                    {module, ?MODULE},
                    {line, ?LINE},
                    {expression, (??__Expr)},
                    {mailbox, ?flushMailbox()}
                ]}
            )
        end
    end)()
).

-define(assertNoReceive(),
    ?assertNoReceive(1000)
).

-define(assertNoReceive(__Timeout),
    (fun() ->
        receive
            __Message ->
                erlang:error(
                    {assertNoReceive, [
                        {module, ?MODULE},
                        {line, ?LINE},
                        {mailbox, [__Message | ?flushMailbox()]}
                    ]}
                )
        after __Timeout -> ok
        end
    end)()
).

-define(FUNCTION_NAME_STRING, (erlang:atom_to_binary(?FUNCTION_NAME))).

-endif.
