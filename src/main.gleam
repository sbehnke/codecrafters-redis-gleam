import gleam/bit_array
import gleam/bytes_tree
import gleam/dict
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{None}
import gleam/order
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import glisten.{Packet}

pub type Config {
  Config(version: String, heartbeat_interval: Int, actor_timeout: Int)
}

const config = Config(
  version: "0.3.0",
  heartbeat_interval: 5000,
  actor_timeout: 5000,
)

pub type RespValue {
  SimpleString(String)
  RedisError(String)
  Integer(Int)
  BulkString(String)
  Array(List(RespValue))
  WithTTL(RespValue, timestamp.Timestamp)
  Stream(String, dict.Dict(String, RespValue))
  Null
}

pub type ParseResult {
  ParseResult(value: RespValue, remaining: String)
}

pub fn parse_resp(input: String) -> Result(ParseResult, String) {
  case string.first(input) {
    Ok("+") -> parse_simple_string(string.drop_start(input, 1))
    Ok("-") -> parse_error(string.drop_start(input, 1))
    Ok(":") -> parse_integer(string.drop_start(input, 1))
    Ok("$") -> parse_bulk_string(string.drop_start(input, 1))
    Ok("*") -> parse_array(string.drop_start(input, 1))
    a -> Error("Invalid RESP type indicator: " <> result.unwrap(a, "?"))
  }
}

// Parse simple string (+OK\r\n)
fn parse_simple_string(input: String) -> Result(ParseResult, String) {
  case string.split_once(input, "\r\n") {
    Ok(#(content, remaining)) ->
      Ok(ParseResult(SimpleString(content), remaining))
    Error(_) -> Error("Missing CRLF terminator")
  }
}

// Parse error (-Error message\r\n)
fn parse_error(input: String) -> Result(ParseResult, String) {
  case string.split_once(input, "\r\n") {
    Ok(#(content, remaining)) -> Ok(ParseResult(RedisError(content), remaining))
    Error(_) -> Error("Missing CRLF terminator")
  }
}

// Parse integer (:1000\r\n)
fn parse_integer(input: String) -> Result(ParseResult, String) {
  case string.split_once(input, "\r\n") {
    Ok(#(num_str, remaining)) -> {
      case int.parse(num_str) {
        Ok(num) -> Ok(ParseResult(Integer(num), remaining))
        Error(_) -> Error("Invalid integer format")
      }
    }
    Error(_) -> Error("Missing CRLF terminator")
  }
}

fn parse_bulk_string(input: String) -> Result(ParseResult, String) {
  case string.split_once(input, "\r\n") {
    Ok(#(length_str, rest)) -> {
      case int.parse(length_str) {
        Ok(-1) -> Ok(ParseResult(Null, rest))
        Ok(length) -> {
          // Use string.split_once to find the next \r\n after the content
          let content = string.slice(rest, 0, length)
          let after_content = string.drop_start(rest, length)

          case string.split_once(after_content, "\r\n") {
            Ok(#("", remaining)) -> {
              // The split found \r\n immediately after content (empty string before \r\n)
              Ok(ParseResult(BulkString(content), remaining))
            }
            Ok(#(unexpected, _)) -> {
              Error(
                "Expected CRLF after bulk string content, but found: '"
                <> unexpected
                <> "'",
              )
            }
            Error(_) -> {
              Error("Missing CRLF after bulk string content")
            }
          }
        }
        Error(_) -> Error("Invalid bulk string length: " <> length_str)
      }
    }
    Error(_) -> Error("Missing CRLF after bulk string length")
  }
}

// Parse array (*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n)
fn parse_array(input: String) -> Result(ParseResult, String) {
  case string.split_once(input, "\r\n") {
    Ok(#(count_str, rest)) -> {
      case int.parse(count_str) {
        Ok(-1) -> Ok(ParseResult(Null, rest))
        Ok(count) -> parse_array_elements(rest, count, [])
        Error(_) -> Error("Invalid array count")
      }
    }
    Error(_) -> Error("Missing CRLF after array count")
  }
}

fn parse_array_elements(
  input: String,
  remaining_count: Int,
  acc: List(RespValue),
) -> Result(ParseResult, String) {
  case remaining_count {
    0 -> Ok(ParseResult(Array(list.reverse(acc)), input))
    _ -> {
      case parse_resp(input) {
        Ok(ParseResult(value, rest)) ->
          parse_array_elements(rest, remaining_count - 1, [value, ..acc])
        Error(err) -> Error(err)
      }
    }
  }
}

fn parse_command(input: String) -> Result(List(String), String) {
  case parse_resp(input) {
    Ok(ParseResult(Array(elements), _)) -> {
      extract_strings(elements, [])
    }
    Ok(ParseResult(_, _)) -> Error("Expected array for command")
    Error(err) -> Error(err)
  }
}

fn extract_strings(
  elements: List(RespValue),
  acc: List(String),
) -> Result(List(String), String) {
  case elements {
    [] -> Ok(list.reverse(acc))
    [BulkString(s), ..rest] -> extract_strings(rest, [s, ..acc])
    [SimpleString(s), ..rest] -> extract_strings(rest, [s, ..acc])
    [_, ..] -> Error("Non-string element in command array")
  }
}

// Convert RespValue back to string (for debugging)
pub fn resp_to_string(value: RespValue) -> String {
  case value {
    WithTTL(r, _) -> resp_to_string(r)
    SimpleString(s) -> "+" <> s <> "\r\n"
    RedisError(s) -> "-" <> s <> "\r\n"
    Integer(i) -> ":" <> int.to_string(i) <> "\r\n"
    BulkString(s) ->
      "$" <> int.to_string(string.length(s)) <> "\r\n" <> s <> "\r\n"
    Null -> "$-1\r\n"
    Array(elements) -> {
      let count = int.to_string(list.length(elements))
      let serialized = list.map(elements, resp_to_string) |> string.join("")
      "*" <> count <> "\r\n" <> serialized
    }
    Stream(_id, _elements) -> {
      // TODO: Format stream output
      ""
    }
  }
}

pub fn uppercase_first(arr: List(String)) -> List(String) {
  case arr {
    [] -> []
    [first, ..rest] -> [string.uppercase(first), ..rest]
  }
}

fn send_response(value: RespValue, conn) -> Nil {
  let response = value |> resp_to_string()
  let assert Ok(_) = glisten.send(conn, bytes_tree.from_string(response))
  Nil
}

fn command_to_list(msg: BitArray) -> Result(List(String), String) {
  bit_array.to_string(msg)
  |> result.unwrap("")
  |> parse_command()
}

fn process_ping(conn) {
  SimpleString("PONG") |> send_response(conn)
}

fn process_echo(conn, rest) {
  list.map(rest, fn(s) { BulkString(s) |> send_response(conn) })
  Nil
}

fn process_set_with_ttl(
  conn,
  key: String,
  value: String,
  ttl: String,
  actor_subject,
) {
  let _ = case string.is_empty(ttl) {
    True -> {
      // No TTL
      set_value(actor_subject, key, BulkString(value))
      SimpleString("OK") |> send_response(conn)
    }
    False -> {
      // With TTL
      let ttl =
        ttl
        |> int.parse
        |> result.unwrap(0)
      // |> echo

      let now = timestamp.system_time()
      let expire = timestamp.add(now, duration.milliseconds(ttl))
      let stored = WithTTL(BulkString(value), expire)

      set_value(actor_subject, key, stored)
      SimpleString("OK") |> send_response(conn)
    }
  }
}

fn process_list_length(conn, key: String, actor_subject) {
  get_list_length(actor_subject, key)
  |> result.unwrap(0)
  |> Integer
  |> send_response(conn)
}

fn process_list_pop(conn, key: String, qty: String, actor_subject) {
  let stored = pop_list(actor_subject, key, qty)
  let _ = case stored {
    Error(_) -> Null |> send_response(conn)
    Ok(value) -> value |> send_response(conn)
  }
}

fn process_list_blpop(conn, key: String, timeout: String, actor_subject) {
  let _ = case blocking_get_value(actor_subject, key, timeout) {
    Error(_) -> Null |> send_response(conn)
    Ok(value) -> {
      case value {
        Array(_)
        | BulkString(_)
        | Integer(_)
        | RedisError(_)
        | SimpleString(_)
        | Stream(_, _)
        | Null -> value |> send_response(conn)
        WithTTL(value, e) -> {
          let now = timestamp.system_time()
          case timestamp.compare(now, e) {
            order.Eq | order.Gt -> {
              let _ = delete_value(actor_subject, key)
              Null |> send_response(conn)
            }
            order.Lt -> value |> send_response(conn)
          }
        }
      }
    }
  }
}

fn process_get(conn, key: String, actor_subject) {
  let _ = case get_value(actor_subject, key) {
    Error(_) -> Null |> send_response(conn)
    Ok(value) -> {
      case value {
        Array(_)
        | BulkString(_)
        | Integer(_)
        | RedisError(_)
        | SimpleString(_)
        | Stream(_, _)
        | Null -> value |> send_response(conn)
        WithTTL(value, e) -> {
          let now = timestamp.system_time()
          case timestamp.compare(now, e) {
            order.Eq | order.Gt -> {
              let _ = delete_value(actor_subject, key)
              Null |> send_response(conn)
            }
            order.Lt -> value |> send_response(conn)
          }
        }
      }
    }
  }
}

// string, list, set, zset, hash, and stream.
fn get_resp_type_string(value: RespValue) -> RespValue {
  case value {
    Array(_) -> SimpleString("list")
    BulkString(_) -> SimpleString("string")
    Integer(_) -> SimpleString("string")
    Null -> SimpleString("none")
    RedisError(e) -> RedisError(e)
    SimpleString(_) -> SimpleString("string")
    WithTTL(v, _) -> get_resp_type_string(v)
    Stream(_, _) -> SimpleString("stream")
  }
}

fn process_type(conn, key: String, actor_subject) {
  let _ = case get_value(actor_subject, key) {
    Error(_) -> SimpleString("none") |> send_response(conn)
    Ok(value) -> {
      get_resp_type_string(value)
      |> send_response(conn)
    }
  }
}

fn process_stream_add(conn, key, id, rest, actor_subject) {
  // entries:
  //   - id: 1526985054069-0 # (ID of the first entry)
  //     temperature: 36 # (A key value pair in the first entry)
  //     humidity: 95 # (Another key value pair in the first entry)

  //   - id: 1526985054079-0 # (ID of the second entry)
  //     temperature: 37 # (A key value pair in the first entry)
  //     humidity: 94 # (Another key value pair in the first entry)

  //   # ... (and so on)
  //
  let pairs = rest |> list.sized_chunk(2)
  let values: dict.Dict(String, RespValue) =
    list.fold(pairs, dict.new(), fn(acc, p) {
      case p {
        [key, value] -> {
          dict.insert(acc, key, BulkString(value))
        }
        _ -> acc
      }
    })

  let stored = Stream(id, values)
  set_value(actor_subject, key, stored)
  send_response(BulkString(id), conn)
  Nil
}

fn process_list_append(conn, key, values: List(String), actor_subject) {
  let _ = case list.is_empty(values) {
    False -> {
      let length =
        get_list_length(actor_subject, key)
        |> result.unwrap(0)

      list.map(values, fn(value) {
        append_list_value(actor_subject, key, BulkString(value))
      })

      length + list.length(values)
      |> Integer
      |> send_response(conn)
    }
    True -> RedisError("Cannot push empty list") |> send_response(conn)
  }
}

fn process_list_prepend(conn, key, values: List(String), actor_subject) {
  let _ = case list.is_empty(values) {
    False -> {
      list.map(values, fn(value) {
        prepend_list_value(actor_subject, key, BulkString(value))
      })
      get_list_length(actor_subject, key)
      |> result.unwrap(0)
      |> Integer
      |> send_response(conn)
    }
    True -> {
      RedisError("Cannot push empty list") |> send_response(conn)
    }
  }
}

fn process_list_range(conn, key, from: String, to: String, actor_subject) {
  let _ = case get_value(actor_subject, key) {
    Error(_) -> {
      Array([]) |> send_response(conn)
    }
    Ok(value) -> {
      case value {
        Array(s) -> {
          let from_raw = from |> int.parse() |> result.unwrap(0)
          let to_raw = to |> int.parse() |> result.unwrap(0)
          let length = list.length(s)

          // Convert negative indices to positive
          let from_normalized = case from_raw < 0 {
            True -> length + from_raw
            False -> from_raw
          }

          let to_normalized = case to_raw < 0 {
            True -> length + to_raw
            False -> to_raw
          }

          let from_clamped = case from_normalized < 0 {
            True -> 0
            False ->
              case from_normalized >= length {
                True -> length
                False -> from_normalized
              }
          }

          let to_clamped = case to_normalized < 0 {
            True -> -1
            False ->
              case to_normalized >= length {
                True -> length - 1
                False -> to_normalized
              }
          }

          case from_clamped <= to_clamped {
            True -> {
              let indices = list.range(from_clamped, to_clamped)
              let items =
                list.fold(indices, [], fn(acc, index) {
                  case list.drop(s, index) |> list.first() {
                    Error(_) -> acc
                    Ok(v) -> list.append(acc, [v])
                  }
                })
              Array(items) |> send_response(conn)
            }
            False -> Array([]) |> send_response(conn)
          }
        }
        _ -> Array([]) |> send_response(conn)
      }
    }
  }
  Nil
}

fn process_unknown_command(conn, cmd) {
  RedisError("Unsupported command " <> cmd) |> send_response(conn)
}

type State {
  State(
    data: dict.Dict(String, RespValue),
    blpop_clients: dict.Dict(
      String,
      List(process.Subject(Result(RespValue, Nil))),
    ),
  )
}

type Message {
  Heartbeat(subject: process.Subject(Message), delay: Int)
  Delete(key: String)
  Get(key: String, reply_with: process.Subject(Result(RespValue, Nil)))
  BlockingGet(key: String, reply_with: process.Subject(Result(RespValue, Nil)))
  RemoveBlockingClient(
    key: String,
    reply_with: process.Subject(Result(RespValue, Nil)),
  )
  Set(key: String, value: RespValue)
  ListAdded(key: String)
  AppendList(key: String, value: RespValue)
  PrependList(key: String, value: RespValue)
  GetListLength(key: String, reply_with: process.Subject(Result(Int, Nil)))
  PopList(
    key: String,
    qty: String,
    reply_with: process.Subject(Result(RespValue, Nil)),
  )
}

fn on_heartbeat(subject, delay, state: State) -> actor.Next(State, b) {
  let _now =
    timestamp.system_time()
    |> timestamp.to_rfc3339(duration.hours(0))
    |> string.pad_end(to: 30, with: "0")
  // |> echo
  // io.println("[" <> now <> "] Actor heartbeat...")

  let new_data =
    dict.filter(state.data, fn(key, value) -> Bool {
      case value {
        Array(_)
        | BulkString(_)
        | Integer(_)
        | Null
        | RedisError(_)
        | Stream(_, _)
        | SimpleString(_) -> {
          True
        }
        WithTTL(_, expire) -> {
          case
            duration.to_seconds(timestamp.difference(
              timestamp.system_time(),
              expire,
            ))
            >=. 0.0
          {
            False -> {
              io.println("TTL expired for: " <> key)
              False
            }
            True -> True
          }
        }
      }
    })
  let new_state = State(..state, data: new_data)
  process.send_after(subject, delay, Heartbeat(subject, delay))
  actor.continue(new_state)
}

fn on_get(key, client, state: State) -> actor.Next(State, b) {
  let result = dict.get(state.data, key)
  actor.send(client, result)
  actor.continue(state)
}

fn on_blocking_get(
  key: String,
  client: process.Subject(Result(RespValue, Nil)),
  state: State,
) -> actor.Next(State, b) {
  let result = dict.get(state.data, key)
  case result {
    Error(_) -> {
      let new_list = case dict.get(state.blpop_clients, key) {
        Error(_) -> [client]
        Ok(clients) -> [client, ..clients]
      }
      let new_dict = dict.insert(state.blpop_clients, key, new_list)
      let new_state =
        State(..state, blpop_clients: new_dict)
        |> echo
      actor.continue(new_state)
    }
    Ok(_) -> {
      on_pop_list_with_listkey(key, client, state)
    }
  }
}

fn on_set(key: String, value: RespValue, state: State) -> actor.Next(State, b) {
  let new_data = dict.insert(state.data, key, value)
  let new_state = State(..state, data: new_data)
  actor.continue(new_state)
}

fn on_list_added(key: String, state: State) -> actor.Next(State, b) {
  echo #(key, "List added")
  let clients = dict.get(state.blpop_clients, key)
  case clients {
    Error(_) -> actor.continue(state)
    Ok(l) -> {
      case l {
        [] -> actor.continue(state)
        [first, ..rest] -> {
          // echo first
          let new_dict = case rest {
            [] -> dict.delete(state.blpop_clients, key)
            _ -> dict.insert(state.blpop_clients, key, rest)
          }
          let new_state = State(state.data, blpop_clients: new_dict)
          on_pop_list_with_listkey(key, first, new_state)
        }
      }
    }
  }
}

fn on_prepend_list(
  key: String,
  value: RespValue,
  state: State,
) -> actor.Next(State, b) {
  let new_data =
    dict.upsert(state.data, key, fn(stored) {
      case stored {
        None -> {
          Array([value])
        }
        option.Some(stored_value) -> {
          case stored_value {
            Array(l) -> {
              Array([value, ..l])
            }
            _ -> {
              Array([])
            }
          }
        }
      }
    })
  let new_state = State(..state, data: new_data)
  on_list_added(key, new_state)
}

fn on_append_list(
  key: String,
  value: RespValue,
  state: State,
) -> actor.Next(State, b) {
  let new_data =
    dict.upsert(state.data, key, fn(stored) {
      case stored {
        None -> {
          Array([value])
        }
        option.Some(stored_value) -> {
          case stored_value {
            Array(l) -> {
              Array(list.append(l, [value]))
            }
            _ -> {
              Array([])
            }
          }
        }
      }
    })
  let new_state = State(..state, data: new_data)
  on_list_added(key, new_state)
}

fn on_get_list_length(
  key: String,
  client: process.Subject(Result(Int, nil)),
  state: State,
) -> actor.Next(State, b) {
  let result = dict.get(state.data, key)
  case result {
    Error(_) -> actor.send(client, Ok(0))
    Ok(stored) -> {
      case stored {
        Array(l) -> {
          actor.send(client, Ok(list.length(l)))
        }
        _ -> {
          actor.send(client, Ok(0))
        }
      }
    }
  }
  actor.continue(state)
}

fn on_pop_list(
  key: String,
  qty: String,
  client: process.Subject(Result(RespValue, Nil)),
  state: State,
) -> actor.Next(State, b) {
  let result = dict.get(state.data, key)
  let new_state = case result {
    Error(_) -> {
      actor.send(client, Ok(Null))
      state
    }
    Ok(stored) -> {
      case stored {
        Array(l) -> {
          let quantity = int.parse(qty) |> result.unwrap(0)
          let new_list = case quantity > 0 {
            True -> {
              let #(popped, remaining) = list.split(l, quantity)
              actor.send(client, Ok(Array(popped)))
              remaining
            }
            False -> {
              case l {
                [first, ..rest] -> {
                  actor.send(client, Ok(first))
                  rest
                }
                [] -> {
                  actor.send(client, Ok(Null))
                  []
                }
              }
            }
          }

          let new_data = case list.is_empty(new_list) {
            False -> dict.insert(state.data, key, Array(new_list))
            True -> dict.delete(state.data, key)
          }
          State(..state, data: new_data)
        }
        _ -> state
      }
    }
  }

  actor.continue(new_state)
}

fn on_pop_list_with_listkey(
  key: String,
  client: process.Subject(Result(RespValue, Nil)),
  state: State,
) -> actor.Next(State, b) {
  let result = dict.get(state.data, key)
  let new_state = case result {
    Error(_) -> {
      actor.send(client, Ok(Null))
      state
    }
    Ok(stored) -> {
      case stored {
        Array(l) -> {
          let new_list = case l {
            [first, ..rest] -> {
              actor.send(client, Ok(Array([BulkString(key), first])))
              rest
            }
            [] -> {
              actor.send(client, Ok(Null))
              []
            }
          }

          let new_data = case list.is_empty(new_list) {
            False -> dict.insert(state.data, key, Array(new_list))
            True -> dict.delete(state.data, key)
          }
          State(..state, data: new_data)
        }
        _ -> state
      }
    }
  }

  actor.continue(new_state)
}

fn on_delete(key: String, state: State) -> actor.Next(State, b) {
  let new_data = dict.delete(state.data, key)
  let new_state = State(..state, data: new_data)
  actor.continue(new_state)
}

fn on_remove_blocking_client(
  key: String,
  client: process.Subject(Result(RespValue, Nil)),
  state: State,
) -> actor.Next(State, b) {
  echo #("Client removing: ", client)
  let new_list = case dict.get(state.blpop_clients, key) {
    Error(_) -> []
    Ok(clients) -> {
      list.filter(clients, fn(c) { c == client })
    }
  }
  let new_dict = case list.is_empty(new_list) {
    False -> dict.insert(state.blpop_clients, key, new_list)
    True -> dict.delete(state.blpop_clients, key)
  }
  let new_state = State(..state, blpop_clients: new_dict)
  actor.send(client, Ok(Null))
  echo #("New State: ", new_state)
  actor.continue(new_state)
}

// Actor loop function
fn handle_message(state: State, message: Message) -> actor.Next(State, b) {
  // echo message
  case message {
    Heartbeat(subject, delay) -> on_heartbeat(subject, delay, state)
    Get(key, client) -> on_get(key, client, state)
    BlockingGet(key, client) -> on_blocking_get(key, client, state)
    RemoveBlockingClient(key, client) ->
      on_remove_blocking_client(key, client, state)
    Set(key, value) -> on_set(key, value, state)
    ListAdded(key) -> on_list_added(key, state)
    PrependList(key, value) -> on_prepend_list(key, value, state)
    AppendList(key, value) -> on_append_list(key, value, state)
    GetListLength(key, client) -> on_get_list_length(key, client, state)
    PopList(key, qty, client) -> on_pop_list(key, qty, client, state)
    Delete(key) -> on_delete(key, state)
  }
}

// Start the actor
fn start() {
  let assert Ok(actor) =
    actor.new(State(data: dict.new(), blpop_clients: dict.new()))
    |> actor.on_message(handle_message)
    |> actor.start()
  actor.data
}

// Get a value from the actor (blocks until response)
fn get_value(
  actor_subject: process.Subject(Message),
  key: String,
) -> Result(RespValue, Nil) {
  actor.call(actor_subject, config.actor_timeout, Get(key, _))
}

//
// This might have a memory leak
// When calling BLPOP that results in blocking then the ListAdd is called
// prior to the timeout, it is possible that the timeout reply might not be
// cleaned up correctly....
//
fn perform_call_with_result(
  subject: process.Subject(message),
  make_request: fn(process.Subject(reply)) -> message,
  run_selector: fn(process.Selector(reply)) -> Result(reply, Nil),
) -> Result(reply, Nil) {
  let reply_subject = process.new_subject()
  let assert Ok(callee) = process.subject_owner(subject)
    as "Callee subject had no owner"

  let monitor = process.monitor(callee)
  process.send(subject, make_request(reply_subject))

  let reply =
    process.new_selector()
    |> process.select(reply_subject)
    |> process.select_specific_monitor(monitor, fn(down) {
      panic as { "callee exited: " <> string.inspect(down) }
    })
    |> run_selector

  process.demonitor_process(monitor)

  // // Optional: Drain any additional messages that might have arrived
  // // This is especially important if you suspect multiple replies
  // case reply {
  //   Ok(_) -> drain_reply_subject(reply_subject)
  //   Error(_) -> drain_reply_subject(reply_subject)
  //   // Also drain on timeout
  // }

  reply
}

// Helper function to drain remaining messages
pub fn drain_reply_subject(subject: process.Subject(reply)) -> Nil {
  let selector =
    process.new_selector()
    |> process.select(subject)

  // Use selector_receive with 0ms timeout - non-blocking
  case process.selector_receive(selector, within: 0) {
    Ok(_) -> drain_reply_subject(subject)
    // Recursively drain
    Error(Nil) -> Nil
    // No more messages (timeout)
  }
}

fn blocking_get_value(
  actor_subject: process.Subject(Message),
  key: String,
  timeout: String,
) -> Result(RespValue, Nil) {
  let delay =
    { float.parse(timeout) |> result.unwrap(0.0) } *. 1000.0 |> float.round()
  case delay == 0 {
    False -> {
      case
        perform_call_with_result(
          actor_subject,
          BlockingGet(key, _),
          process.selector_receive(_, delay),
        )
      {
        Error(_) -> {
          process.call(
            actor_subject,
            config.actor_timeout,
            RemoveBlockingClient(key, _),
          )
        }
        Ok(r) -> r
      }
    }
    True -> process.call_forever(actor_subject, BlockingGet(key, _))
  }
}

fn delete_value(actor_subject: process.Subject(Message), key: String) -> Nil {
  actor.send(actor_subject, Delete(key))
}

// Set a value (fire and forget)
fn set_value(
  actor_subject: process.Subject(Message),
  key: String,
  value: RespValue,
) -> Nil {
  actor.send(actor_subject, Set(key, value))
}

// Set a value (fire and forget)
fn append_list_value(
  actor_subject: process.Subject(Message),
  key: String,
  value: RespValue,
) -> Nil {
  actor.send(actor_subject, AppendList(key, value))
}

fn prepend_list_value(
  actor_subject: process.Subject(Message),
  key: String,
  value: RespValue,
) -> Nil {
  actor.send(actor_subject, PrependList(key, value))
}

fn get_list_length(
  actor_subject: process.Subject(Message),
  key: String,
) -> Result(Int, Nil) {
  actor.call(actor_subject, config.actor_timeout, GetListLength(key, _))
}

fn pop_list(
  actor_subject: process.Subject(Message),
  key: String,
  qty: String,
) -> Result(RespValue, Nil) {
  actor.call(actor_subject, config.actor_timeout, PopList(key, qty, _))
}

pub fn main() {
  io.println("Redis: Gleam Edition " <> config.version)

  let actor_subject = start()
  process.send_after(
    actor_subject,
    config.heartbeat_interval,
    Heartbeat(actor_subject, config.heartbeat_interval),
  )
  let assert Ok(_) =
    glisten.new(fn(_conn) { #(dict.new(), None) }, fn(state, msg, conn) {
      let assert Packet(msg) = msg
      let commands = command_to_list(msg)

      case commands {
        Error(_) -> glisten.continue(state)
        Ok(command) -> {
          command |> echo
          case command |> uppercase_first() {
            ["PING"] -> process_ping(conn)
            ["ECHO", ..rest] -> process_echo(conn, rest)
            ["SET", key, value] ->
              process_set_with_ttl(conn, key, value, "", actor_subject)
            ["SET", key, value, "px", ttl] ->
              process_set_with_ttl(conn, key, value, ttl, actor_subject)
            ["GET", key] -> process_get(conn, key, actor_subject)
            // RPUSH list_key "foo"
            ["RPUSH", key, ..values] ->
              process_list_append(conn, key, values, actor_subject)
            //LPUSH list_key "a" "b" "c"
            ["LPUSH", key, ..values] ->
              process_list_prepend(conn, key, values, actor_subject)
            ["LRANGE", key, from, to] ->
              process_list_range(conn, key, from, to, actor_subject)
            ["LLEN", key] -> process_list_length(conn, key, actor_subject)
            ["LPOP", key] -> process_list_pop(conn, key, "", actor_subject)
            ["LPOP", key, qty] ->
              process_list_pop(conn, key, qty, actor_subject)
            ["BLPOP", key, timeout] ->
              process_list_blpop(conn, key, timeout, actor_subject)
            ["TYPE", key] -> process_type(conn, key, actor_subject)
            // redis-cli XADD stream_key 1526919030474-0 temperature 36 humidity 95
            ["XADD", key, id, ..rest] ->
              process_stream_add(conn, key, id, rest, actor_subject)
            [c] -> process_unknown_command(conn, c)
            _ -> process_unknown_command(conn, "UNKNOWN")
          }
          glisten.continue(state)
        }
      }
    })
    |> glisten.bind("0.0.0.0")
    |> glisten.with_close(fn(state) {
      echo state
      Nil
    })
    |> glisten.start(6379)

  process.sleep_forever()
}
