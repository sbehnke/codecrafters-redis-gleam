import gleam/bit_array
import gleam/bytes_tree
import gleam/dict
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{None}
import gleam/otp/actor
import gleam/result
import gleam/string
import glisten.{Packet}

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

fn process_set(conn, key: String, value: String, actor_subject) {
  set_value(actor_subject, key, value)
  let response = SimpleString("OK") |> resp_to_string()
  let assert Ok(_) = glisten.send(conn, bytes_tree.from_string(response))
  Nil
}

fn process_get(conn, key: String, actor_subject) {
  let _ = case get_value(actor_subject, key) {
    Error(_) -> {
      let response = Null |> resp_to_string()
      let assert Ok(_) = glisten.send(conn, bytes_tree.from_string(response))
    }
    Ok(value) -> {
      let response = BulkString(value) |> resp_to_string()
      let assert Ok(_) = glisten.send(conn, bytes_tree.from_string(response))
    }
  }
  Nil
}

// Define your actor state
pub type State {
  State(data: dict.Dict(String, String))
}

// Define your message types
pub type Message {
  Get(key: String, reply_with: process.Subject(Result(String, Nil)))
  Set(key: String, value: String)
}

// Actor loop function
fn handle_message(state: State, message: Message) {
  case message {
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
) -> Result(String, Nil) {
  actor.call(actor_subject, 5000, Get(key, _))
  // 5 second timeout
}

// Set a value (fire and forget)
pub fn set_value(
  actor_subject: process.Subject(Message),
  key: String,
  value: String,
) -> Nil {
  actor.send(actor_subject, Set(key, value))
}

pub fn main() {
  io.println("Redis: Gleam Edition 0.0.3")

  let actor_subject = start()
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
            ["SET", key, value] -> process_set(conn, key, value, actor_subject)
            ["GET", key] -> process_get(conn, key, actor_subject)
            _ -> Nil
          }
          glisten.continue(state)
        }
      }
    })
    |> glisten.bind("0.0.0.0")
    |> glisten.start(6379)

  process.sleep_forever()
}
