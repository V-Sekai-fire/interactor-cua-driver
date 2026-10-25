# Collects decider examples through the running cua-tracer daemon: real actions on Calculator and a
# TextEdit document, each a traced span whose views and effect land in the daemon's trace store.
#
#   elixir collect.exs <actions> [seed]
#
# Actions, chosen in turn at random:
#   calc_press  press a keypad button by element token   (the driver cannot read a press back)
#   text_set    set_value on the document's text area   (read back: confirmed or not)
#   text_type   type_text into the document's text area (read back: confirmed or not)
# Each result's effect is appended to collect.jsonl next to this script. Only keypad buttons are
# pressed, never window controls.

defmodule Collect do
  @keypad ~w(Zero One Two Three Four Five Six Seven Eight Nine Add Subtract Multiply Divide Equals
             Decimal Negate Percent Delete AllClear)
  @words ~w(decider gold view before after window span trace state measure answer option frame)
  @kinds [:calc_press, :text_set, :text_type]

  def main([count | rest]) do
    seed = case rest do
      [s] -> String.to_integer(s)
      [] -> 1
    end

    :rand.seed(:exsss, {seed, seed, seed})
    log = Path.join(Path.dirname(__ENV__.file), "collect.jsonl")

    with {:ok, exe} <- find("cua-tracer"),
         {:ok, socket} <- {:ok, Path.expand("~/.cache/cua-tracer/cua-tracer.sock")},
         {:ok, windows} <- call(exe, socket, "list_windows", %{}),
         {:ok, calc} <- window(windows, "Calculator"),
         {:ok, text} <- window(windows, "TextEdit") do
      targets = %{calc: calc, text: text}

      results =
        Enum.map(1..String.to_integer(count), fn i ->
          kind = Enum.random(@kinds)
          result = act(exe, socket, kind, targets)
          line = :json.encode(%{"i" => i, "seed" => seed, "kind" => kind, "outcome" => outcome(result)})
          File.write(log, [line, "\n"], [:append])
          {kind, outcome(result)}
        end)

      results
      |> Enum.frequencies()
      |> Enum.sort()
      |> Enum.each(fn {{kind, out}, n} -> IO.puts("#{kind} #{out}: #{n}") end)

      :ok
    end
  end

  def main(_), do: {:error, :usage}

  defp outcome({:ok, %{"effect" => e}}), do: e
  defp outcome({:ok, _}), do: "no_effect_field"
  defp outcome({:error, {:refused, code}}), do: "refused:#{code}"
  defp outcome({:error, reason}), do: "error:#{inspect(reason) |> String.slice(0, 80)}"

  def act(exe, socket, kind, targets) do
    {pid, wid} = if kind == :calc_press, do: targets.calc, else: targets.text
    base = %{"session" => "collect", "pid" => pid, "window_id" => wid}

    with {:ok, state} <- call(exe, socket, "get_window_state", base),
         {:ok, token} <- token(state, kind) do
      case kind do
        :calc_press -> call(exe, socket, "click", Map.put(base, "element_token", token))
        :text_set -> call(exe, socket, "set_value", Map.merge(base, %{"element_token" => token, "value" => phrase()}))
        :text_type -> call(exe, socket, "type_text", Map.merge(base, %{"element_token" => token, "text" => " " <> phrase()}))
      end
    end
  end

  defp phrase, do: Enum.map_join(1..Enum.random(1..4), " ", fn _ -> Enum.random(@words) end)

  # The element token of a keypad button (chosen at random) or of the document's text area.
  defp token(%{"snapshot_id" => snap, "tree_markdown" => tree}, kind) do
    rows =
      tree
      |> String.split("\n")
      |> Enum.flat_map(fn line ->
        case Regex.run(~r/^\s*- \[(\d+)\] (AXButton|AXTextArea)\b.*?\[id=([^\s\]]+)/, line) do
          [_, row, role, id] -> [{row, role, id}]
          nil -> []
        end
      end)

    wanted =
      case kind do
        :calc_press -> Enum.filter(rows, fn {_, role, id} -> role == "AXButton" and id in @keypad end)
        _ -> Enum.filter(rows, fn {_, role, _} -> role == "AXTextArea" end)
      end

    case wanted do
      [] -> {:error, {:no_target, kind}}
      list -> {:ok, "#{snap}:#{elem(Enum.random(list), 0)}"}
    end
  end

  defp token(_, kind), do: {:error, {:no_tree, kind}}

  defp window(%{"windows" => list}, app) do
    case Enum.find(list, &(&1["app_name"] == app and (&1["title"] || "") != "")) do
      nil -> {:error, {:no_window, app}}
      w -> {:ok, {w["pid"], w["window_id"]}}
    end
  end

  defp find(name) do
    case System.find_executable(name) || existing(Path.expand("~/.local/bin/#{name}")) do
      nil -> {:error, {:not_found, name}}
      path -> {:ok, path}
    end
  end

  defp existing(path), do: if(File.exists?(path), do: path, else: nil)

  # One tool call; a refusal or a non-JSON answer comes back as an error, never a raise.
  def call(exe, socket, tool, args) do
    {out, code} = System.cmd(exe, ["call", tool, :json.encode(args) |> IO.iodata_to_binary(), "--socket=#{socket}"], stderr_to_stdout: true)
    text = String.trim(out)

    cond do
      code != 0 -> {:error, {:exit, code, String.slice(text, 0, 120)}}
      not String.starts_with?(text, "{") -> {:error, {:not_json, String.slice(text, 0, 120)}}
      true ->
        case :json.decode(text) do
          %{"status" => "refused", "refusal" => %{"code" => c}} -> {:error, {:refused, c}}
          map -> {:ok, Map.get(map, "structuredContent", map) |> flatten_effect(map)}
        end
    end
  end

  # The effect sits at the top level or under the action record, depending on the tool.
  defp flatten_effect(inner, outer) do
    effect = inner["effect"] || get_in(outer, ["action_record", "effect"]) || outer["effect"]
    if effect, do: Map.put(inner, "effect", effect), else: inner
  end
end

case Collect.main(System.argv()) do
  :ok -> :ok
  {:error, :usage} -> IO.puts(:stderr, "usage: elixir collect.exs <actions> [seed]") && System.halt(2)
  {:error, reason} -> IO.puts(:stderr, "collect failed: #{inspect(reason)}") && System.halt(1)
end
