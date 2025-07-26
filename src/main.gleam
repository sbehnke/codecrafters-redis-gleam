import gleam/bit_array
import gleam/bytes_tree
import gleam/dict
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{None, Some}
import gleam/order
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import glisten.{Packet}

const version = "0.0.6"

const heartbeat_interval = 5000

const actor_timeout = 5000

pub type StoredValue {
  StoredString(String)
  StoredList(List(StoredValue))
  StoredStringWithTTL(String, timestamp.Timestamp)
}

pub type RespValue {
  SimpleString(String)
  RedisError(String)
  Integer(Int)
  BulkString(String)
  Array(List(RespValue))
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

// Parse your specific example
pub fn parse_command(input: String) -> Result(List(String), String) {
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
  }
}

pub fn uppercase_first(arr: List(String)) -> List(String) {
  case arr {
    [] -> []
    [first, ..rest] -> [string.uppercase(first), ..rest]
  }
}

fn command_to_list(msg: BitArray) -> Result(List(String), String) {
  bit_array.to_string(msg)
  |> result.unwrap("")
  |> parse_command()
}

fn process_ping(conn) {
  let response = SimpleString("PONG") |> resp_to_string()
  let assert Ok(_) = glisten.send(conn, bytes_tree.from_string(response))
  Nil
}

fn process_echo(conn, rest) {
  list.map(rest, fn(s) {
    let response = BulkString(s) |> resp_to_string()
    let assert Ok(_) = glisten.send(conn, bytes_tree.from_string(response))
  })
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
      set_value(actor_subject, key, StoredString(value))
      let response = SimpleString("OK") |> resp_to_string()
      let assert Ok(_) = glisten.send(conn, bytes_tree.from_string(response))
    }
    False -> {
      // With TTL
      let ttl =
        ttl
        |> int.parse
        |> result.unwrap(0)
        |> echo

      let now = timestamp.system_time()
      let expire = timestamp.add(now, duration.milliseconds(ttl))
      let stored = StoredStringWithTTL(value, expire)

      set_value(actor_subject, key, stored)
      let response = SimpleString("OK") |> resp_to_string()
      let assert Ok(_) = glisten.send(conn, bytes_tree.from_string(response))
    }
  }
  Nil
}

fn process_get(conn, key: String, actor_subject) {
  let _ = case get_value(actor_subject, key) {
    Error(_) -> {
      let response = Null |> resp_to_string()
      let assert Ok(_) = glisten.send(conn, bytes_tree.from_string(response))
    }
    Ok(value) -> {
      case value {
        StoredList(_) -> {
          let response = RedisError("Unimplemented") |> resp_to_string()
          let assert Ok(_) =
            glisten.send(conn, bytes_tree.from_string(response))
        }
        StoredString(s) -> {
          let response = BulkString(s) |> resp_to_string()
          let assert Ok(_) =
            glisten.send(conn, bytes_tree.from_string(response))
        }
        StoredStringWithTTL(s, e) -> {
          let now = timestamp.system_time()
          case timestamp.compare(now, e) {
            order.Eq | order.Gt -> {
              let _ = delete_value(actor_subject, key)
              let response = Null |> resp_to_string()
              let assert Ok(_) =
                glisten.send(conn, bytes_tree.from_string(response))
            }
            order.Lt -> {
              let response = BulkString(s) |> resp_to_string()
              let assert Ok(_) =
                glisten.send(conn, bytes_tree.from_string(response))
            }
          }
        }
      }
    }
  }
  Nil
}

pub fn process_list_append(conn, key, values: List(String), actor_subject) {
  let _ = case list.is_empty(values) {
    False -> {
      list.map(values, fn(value) {
        append_list_value(actor_subject, key, StoredString(value))
      })
      let length = get_list_length(actor_subject, key) |> result.unwrap(0)
      let response = Integer(length) |> resp_to_string()
      let assert Ok(_) = glisten.send(conn, bytes_tree.from_string(response))
    }
    True -> {
      let response = RedisError("Cannot push empty list") |> resp_to_string()
      let assert Ok(_) = glisten.send(conn, bytes_tree.from_string(response))
    }
  }

  Nil
}

// Legacy list.at which shouldn't actually be used anymore due to performace
pub fn list_at(in list: List(a), get index: Int) -> option.Option(a) {
  case index < 0 {
    True -> None
    False ->
      case list {
        [] -> None
        [x, ..rest] ->
          case index == 0 {
            True -> Some(x)
            False -> list_at(rest, index - 1)
          }
      }
  }
}

pub fn process_list_range(conn, key, from: String, to: String, actor_subject) {
  let _ = case get_value(actor_subject, key) {
    Error(_) -> {
      let response = Array([]) |> resp_to_string()
      let assert Ok(_) = glisten.send(conn, bytes_tree.from_string(response))
    }
    Ok(value) -> {
      case value {
        StoredList(s) -> {
          let from = from |> int.parse() |> result.unwrap(0)
          let to = to |> int.parse() |> result.unwrap(0)
          let length = list.length(s)

          let #(to, from) = case to < 0, from < 0 {
            True, True -> #(length + to, length + from)
            True, False -> #(length + to, from)
            False, True -> #(to, from + length)
            False, False -> #(to, from)
          }

          case from > to {
            False -> {
              let range = list.range(from, to)
              let items =
                list.fold(range, [], fn(acc, index) {
                  let item = case list_at(s, index) {
                    None -> None
                    Some(v) -> {
                      case v {
                        StoredString(s) -> Some(BulkString(s))
                        _ -> None
                      }
                    }
                  }
                  case item {
                    None -> acc
                    Some(item) -> {
                      list.append(acc, [item])
                    }
                  }
                })

              let response = Array(items) |> resp_to_string()
              let assert Ok(_) =
                glisten.send(conn, bytes_tree.from_string(response))
            }
            True -> {
              let response = Array([]) |> resp_to_string()
              let assert Ok(_) =
                glisten.send(conn, bytes_tree.from_string(response))
            }
          }
        }
        _ -> {
          let response = Array([]) |> resp_to_string()
          let assert Ok(_) =
            glisten.send(conn, bytes_tree.from_string(response))
        }
      }
    }
  }
  Nil
}

pub fn process_unknown_command(conn, cmd) {
  let response = RedisError("Unsupported command " <> cmd) |> resp_to_string()
  let assert Ok(_) = glisten.send(conn, bytes_tree.from_string(response))
  Nil
}

// Define your actor state
pub type State {
  State(data: dict.Dict(String, StoredValue))
}

// Define your message types
pub type Message {
  Heartbeat(subject: process.Subject(Message), delay: Int)
  Delete(key: String)
  Get(key: String, reply_with: process.Subject(Result(StoredValue, Nil)))
  Set(key: String, value: StoredValue)
  AppendList(key: String, value: StoredValue)
  GetListLength(key: String, reply_with: process.Subject(Result(Int, Nil)))
}

// Actor loop function
fn handle_message(state: State, message: Message) {
  case message {
    Heartbeat(subject, delay) -> {
      let now =
        timestamp.system_time()
        |> timestamp.to_rfc3339(duration.hours(0))
        |> string.pad_end(to: 30, with: "0")
      io.println("[" <> now <> "] Actor heartbeat...")

      let new_data =
        dict.filter(state.data, fn(_, value) -> Bool {
          case value {
            StoredList(_) -> True
            StoredString(_) -> True
            StoredStringWithTTL(key, expire) -> {
              case
                duration.to_seconds(timestamp.difference(
                  timestamp.system_time(),
                  expire,
                ))
                >=. 0.0
              {
                False -> {
                  io.println("TTL Expired for: " <> key)
                  False
                }
                True -> True
              }
            }
          }
        })
      let new_state = State(data: new_data)
      process.send_after(subject, delay, Heartbeat(subject, delay))
      actor.continue(new_state)
    }
    Get(key, client) -> {
      let result = dict.get(state.data, key)
      actor.send(client, result)
      actor.continue(state)
    }
    Set(key, value) -> {
      let new_data = dict.insert(state.data, key, value)
      let new_state = State(data: new_data)
      actor.continue(new_state)
    }
    AppendList(key, value) -> {
      let new_data =
        dict.upsert(state.data, key, fn(stored) {
          case stored {
            None -> {
              StoredList([value])
            }
            option.Some(stored_value) -> {
              case stored_value {
                StoredList(l) -> {
                  StoredList(list.append(l, [value]))
                }
                _ -> {
                  StoredList([])
                }
              }
            }
          }
        })
      let new_state = State(data: new_data)
      actor.continue(new_state)
    }
    GetListLength(key, client) -> {
      let result = dict.get(state.data, key)
      case result {
        Error(_) -> actor.send(client, Ok(0))
        Ok(stored) -> {
          case stored {
            StoredList(l) -> {
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
    Delete(key) -> {
      let new_data = dict.delete(state.data, key)
      let new_state = State(data: new_data)
      actor.continue(new_state)
    }
  }
}

// Start the actor
pub fn start() {
  let assert Ok(actor) =
    actor.new(State(data: dict.new()))
    |> actor.on_message(handle_message)
    |> actor.start()
  actor.data
}

// Get a value from the actor (blocks until response)
pub fn get_value(
  actor_subject: process.Subject(Message),
  key: String,
) -> Result(StoredValue, Nil) {
  actor.call(actor_subject, actor_timeout, Get(key, _))
}

pub fn delete_value(actor_subject: process.Subject(Message), key: String) -> Nil {
  actor.send(actor_subject, Delete(key))
}

// Set a value (fire and forget)
pub fn set_value(
  actor_subject: process.Subject(Message),
  key: String,
  value: StoredValue,
) -> Nil {
  actor.send(actor_subject, Set(key, value))
}

// Set a value (fire and forget)
pub fn append_list_value(
  actor_subject: process.Subject(Message),
  key: String,
  value: StoredValue,
) -> Nil {
  actor.send(actor_subject, AppendList(key, value))
}

pub fn get_list_length(
  actor_subject: process.Subject(Message),
  key: String,
) -> Result(Int, Nil) {
  actor.call(actor_subject, actor_timeout, GetListLength(key, _))
}

pub fn main() {
  io.println("Redis: Gleam Edition " <> version)

  let actor_subject = start()
  process.send_after(
    actor_subject,
    heartbeat_interval,
    Heartbeat(actor_subject, heartbeat_interval),
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
            ["LRANGE", key, from, to] ->
              process_list_range(conn, key, from, to, actor_subject)

            [c] -> process_unknown_command(conn, c)
            _ -> process_unknown_command(conn, "UNKNOWN")
          }
          glisten.continue(state)
        }
      }
    })
    |> glisten.bind("0.0.0.0")
    |> glisten.start(6379)

  process.sleep_forever()
}
