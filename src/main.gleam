// import gleam/bytes_tree
import gleam/bytes_tree
import gleam/erlang/process
import gleam/io
import gleam/option.{None}
import glisten.{Packet}

pub fn main() {
  io.println("Redis: Gleam Edition 0.0.1")

  let assert Ok(_) =
    glisten.new(fn(_conn) { #(Nil, None) }, fn(state, msg, conn) {
      let assert Packet(msg) = msg
      msg
      |> echo

      let _ = case msg {
        <<"*1\r\n$4\r\nPING\r\n">> -> {
          let assert Ok(_) =
            glisten.send(conn, bytes_tree.from_string("+PONG\r\n"))
          Nil
        }
        _ -> Nil
      }
      // case <<"*1\r\n$4\r\nPING\r\n">> -> {
      //   let assert Ok(_) = glisten.send(conn, bytes_tree.from_bit_array(msg))
      // }

      // let assert Ok(_) = glisten.send(conn, bytes_tree.from_bit_array(msg))
      glisten.continue(state)
    })
    |> glisten.bind("0.0.0.0")
    |> glisten.start(6379)

  process.sleep_forever()
}
