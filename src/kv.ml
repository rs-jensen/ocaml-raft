module S = Map.Make (String)

type t = int S.t
type op = Put of int | Add of int | Del
type command = { id : int; key : string; op : op }
type result = int option

let empty = S.empty

let apply m c =
  match c.op with
  | Put v -> (S.add c.key v m, Some v)
  | Add d ->
    let old = match S.find_opt c.key m with Some v -> v | None -> 0 in
    (S.add c.key (old + d) m, Some (old + d))
  | Del -> (S.remove c.key m, None)

let get m key = S.find_opt key m
let equal a b = S.equal Int.equal a b
let to_list m = S.bindings m
