// import gleam/bytes_tree
import gleam/erlang/process
import gleam/option.{None}
import glisten.{Packet}

pub fn main() {
  let assert Ok(_) =
    glisten.new(fn(_conn) { #(Nil, None) }, fn(state, msg, _conn) {
      let assert Packet(_) = msg
      // let assert Ok(_) = glisten.send(conn, bytes_tree.from_bit_array(msg))
      glisten.continue(state)
    })
    |> glisten.bind("0.0.0.0")
    |> glisten.start(6379)

  process.sleep_forever()
}
