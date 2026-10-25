# Builds the Hugging Face sample of decider examples (RFD66606-2334) from a cua-tracer trace store
# (schema 5, whose sessions, spans and views already carry their UUIDv7s).
#
#   elixir build_sample.exs <trace-store.sqlite> <stage dir> <collector seed> <driver commit>
#
# Writes Parquet (zstd) under <stage dir>:
#   etnf/<table>.parquet            the store's tables in ETNF. Sessions, spans and views are keyed
#                                   by the UUIDv7 (RFC 9562) the store gave them when it wrote them,
#                                   so a rebuild keeps every id; vocabularies (tools, roles,
#                                   questions, metrics) by their names.
#   decider_denormalized.parquet    one row per decider example, for the dataset viewer: a view
#                                   over the ETNF tables, not canonical.
# Before writing it checks every primary key is unique and every foreign key resolves, with a
# planted duplicate and a planted orphan that must both be caught. The store is opened read-only.

Mix.install([{:explorer, "~> 0.11"}, {:exqlite, "~> 0.27"}])

defmodule BuildSample do
  alias Explorer.DataFrame, as: DF
  alias Explorer.Series

  @concept "urn:oid:1.3.6.1.4.1.66606.1.2.2334"

  def main([store, stage, seed, commit]) do
    with true <- File.exists?(store) || {:error, {:no_store, store}},
         {:ok, conn} <- Exqlite.Sqlite3.open(store, mode: :readonly),
         {:ok, _} <- conn |> query("PRAGMA user_version") |> schema(),
         {:ok, raw} <- read_all(conn),
         {:ok, sha} <- sha256(store),
         {:ok, tables} <- etnf(raw, %{store_sha256: sha, seed: seed, commit: commit}),
         :ok <- check(tables),
         :ok <- File.mkdir_p(Path.join(stage, "etnf")),
         :ok <- write_all(tables, stage) do
      write(denormalize(tables), Path.join(stage, "decider_denormalized.parquet"))
    end
  end

  def main(_), do: {:error, :usage}

  # --- read -------------------------------------------------------------------------------

  @raw ~w(session span span_parent span_end span_detail span_command tool task_type asset_kind
          asset asset_extent input_role metric question question_option decider_example
          decider_input decider_question decider_gold decider_measure)

  defp read_all(conn) do
    Enum.reduce_while(@raw, {:ok, %{}}, fn t, {:ok, acc} ->
      case query(conn, "SELECT * FROM #{t}") do
        {:ok, rows} -> {:cont, {:ok, Map.put(acc, t, rows)}}
        error -> {:halt, error}
      end
    end)
  end

  defp query(conn, sql) do
    with {:ok, stmt} <- Exqlite.Sqlite3.prepare(conn, sql),
         {:ok, cols} <- Exqlite.Sqlite3.columns(conn, stmt),
         {:ok, rows} <- Exqlite.Sqlite3.fetch_all(conn, stmt),
         :ok <- Exqlite.Sqlite3.release(conn, stmt) do
      {:ok, Enum.map(rows, fn r -> cols |> Enum.zip(r) |> Map.new() end)}
    end
  end

  defp schema({:ok, [%{"user_version" => 5}]} = ok), do: ok
  defp schema({:ok, [%{"user_version" => v}]}), do: {:error, {:schema, v, :want, 5}}
  defp schema(error), do: error

  defp sha256(path) do
    case File.read(path) do
      {:ok, bytes} -> {:ok, :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)}
      error -> error
    end
  end

  # --- ETNF -------------------------------------------------------------------------------

  defp etnf(raw, provenance) do
    name = fn t, key -> Map.new(raw[t], &{&1[key], &1["name"]}) end
    tool = name.("tool", "tool_id")
    task = name.("task_type", "task_type_id")
    kind = name.("asset_kind", "asset_kind_id")
    role = name.("input_role", "input_role_id")
    metric = name.("metric", "metric_id")
    question = name.("question", "question_id")

    session_uuid = Map.new(raw["session"], &{&1["session_id"], &1["uuid"]})
    span_uuid = Map.new(raw["span"], &{&1["span_id"], &1["uuid"]})
    asset_uuid = Map.new(raw["asset"], &{&1["asset_id"], &1["uuid"]})

    s = &span_uuid[&1]

    tables = %{
      "source" => [
        %{
          "concept" => @concept,
          "schema_version" => 5,
          "store_sha256" => provenance.store_sha256,
          "driver" => "V-Sekai-fire/interactor-cua-driver",
          "driver_commit" => provenance.commit,
          "collector" => "libs/cua-driver/tools/decider/collect.exs",
          "collector_seed" => String.to_integer(provenance.seed),
          "host_os" => "macOS",
          "built_at" => DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()
        }
      ],
      "session" =>
        Enum.map(
          raw["session"],
          &%{"session_uuid" => session_uuid[&1["session_id"]], "started_at" => &1["started_at"]}
        ),
      "span" =>
        Enum.map(raw["span"], fn r ->
          %{
            "span_uuid" => s.(r["span_id"]),
            "session_uuid" => session_uuid[r["session_id"]],
            "span_key" => r["span_key"],
            "tool" => tool[r["tool_id"]],
            "started" => r["started"]
          }
        end),
      "span_parent" =>
        Enum.map(
          raw["span_parent"],
          &%{"span_uuid" => s.(&1["span_id"]), "parent_span_uuid" => s.(&1["parent_span_id"])}
        ),
      "span_end" =>
        Enum.map(
          raw["span_end"],
          &%{"span_uuid" => s.(&1["span_id"]), "ended" => &1["ended"], "ok" => &1["ok"] == 1}
        ),
      "span_detail" =>
        Enum.map(
          raw["span_detail"],
          &%{"span_uuid" => s.(&1["span_id"]), "detail" => &1["detail"]}
        ),
      "span_command" =>
        Enum.map(raw["span_command"], fn r ->
          %{
            "span_uuid" => s.(r["span_id"]),
            "seq" => r["seq"],
            "at" => r["at"],
            "command" => r["command"],
            "ok" => r["ok"] == 1
          }
        end),
      "asset" =>
        Enum.map(raw["asset"], fn r ->
          %{
            "asset_uuid" => asset_uuid[r["asset_id"]],
            "asset_kind" => kind[r["asset_kind_id"]],
            "bytes" => {:blob, r["bytes"]}
          }
        end),
      "asset_extent" =>
        Enum.map(
          raw["asset_extent"],
          &%{
            "asset_uuid" => asset_uuid[&1["asset_id"]],
            "width" => &1["width"],
            "height" => &1["height"]
          }
        ),
      "question" => Enum.map(raw["question"], &%{"question" => &1["name"], "text" => &1["text"]}),
      "question_option" =>
        Enum.map(raw["question_option"], fn r ->
          %{
            "question" => question[r["question_id"]],
            "option_index" => r["option_index"],
            "label" => r["label"]
          }
        end),
      "decider_example" =>
        Enum.map(
          raw["decider_example"],
          &%{
            "span_uuid" => s.(&1["span_id"]),
            "concept" => @concept,
            "task_type" => task[&1["task_type_id"]]
          }
        ),
      "decider_input" =>
        Enum.map(raw["decider_input"], fn r ->
          %{
            "span_uuid" => s.(r["span_id"]),
            "input_role" => role[r["input_role_id"]],
            "asset_uuid" => asset_uuid[r["asset_id"]]
          }
        end),
      "decider_question" =>
        Enum.map(
          raw["decider_question"],
          &%{"span_uuid" => s.(&1["span_id"]), "question" => question[&1["question_id"]]}
        ),
      "decider_gold" =>
        Enum.map(raw["decider_gold"], fn r ->
          %{
            "span_uuid" => s.(r["span_id"]),
            "question" => question[r["question_id"]],
            "option_index" => r["option_index"]
          }
        end),
      "decider_measure" =>
        Enum.map(
          raw["decider_measure"],
          &%{
            "span_uuid" => s.(&1["span_id"]),
            "metric" => metric[&1["metric_id"]],
            "value" => &1["value"]
          }
        )
    }

    # Empty tables are left out.
    {:ok, tables |> Enum.reject(fn {_, rows} -> rows == [] end) |> Map.new()}
  end

  # --- keys -------------------------------------------------------------------------------

  @pk %{
    "source" => ["store_sha256"],
    "session" => ["session_uuid"],
    "span" => ["span_uuid"],
    "span_parent" => ["span_uuid"],
    "span_end" => ["span_uuid"],
    "span_detail" => ["span_uuid"],
    "span_command" => ["span_uuid", "seq"],
    "asset" => ["asset_uuid"],
    "asset_extent" => ["asset_uuid"],
    "question" => ["question"],
    "question_option" => ["question", "option_index"],
    "decider_example" => ["span_uuid"],
    "decider_input" => ["span_uuid", "input_role"],
    "decider_question" => ["span_uuid", "question"],
    "decider_gold" => ["span_uuid", "question"],
    "decider_measure" => ["span_uuid", "metric"]
  }

  @fk [
    {"span", ["session_uuid"], "session", ["session_uuid"]},
    {"span_parent", ["span_uuid"], "span", ["span_uuid"]},
    {"span_parent", ["parent_span_uuid"], "span", ["span_uuid"]},
    {"span_end", ["span_uuid"], "span", ["span_uuid"]},
    {"span_detail", ["span_uuid"], "span", ["span_uuid"]},
    {"span_command", ["span_uuid"], "span", ["span_uuid"]},
    {"asset_extent", ["asset_uuid"], "asset", ["asset_uuid"]},
    {"question_option", ["question"], "question", ["question"]},
    {"decider_example", ["span_uuid"], "span", ["span_uuid"]},
    {"decider_input", ["span_uuid"], "decider_example", ["span_uuid"]},
    {"decider_input", ["asset_uuid"], "asset", ["asset_uuid"]},
    {"decider_question", ["span_uuid"], "decider_example", ["span_uuid"]},
    {"decider_question", ["question"], "question", ["question"]},
    {"decider_gold", ["span_uuid", "question"], "decider_question", ["span_uuid", "question"]},
    {"decider_gold", ["question", "option_index"], "question_option",
     ["question", "option_index"]},
    {"decider_measure", ["span_uuid"], "decider_example", ["span_uuid"]}
  ]

  def problems(tables) do
    dup =
      Enum.flat_map(tables, fn {t, rows} ->
        keys = Enum.map(rows, &Map.take(&1, @pk[t]))
        nulls = Enum.any?(keys, fn k -> Enum.any?(Map.values(k), &is_nil/1) end)

        if length(keys) == length(Enum.uniq(keys)) and not nulls,
          do: [],
          else: [{:duplicate_or_null_key, t}]
      end)

    orphan =
      Enum.flat_map(@fk, fn {child, ccols, parent, pcols} ->
        case {tables[child], tables[parent]} do
          {nil, _} ->
            []

          {_, nil} ->
            [{:orphan, child, ccols, parent}]

          {crows, prows} ->
            have = MapSet.new(prows, fn r -> Enum.map(pcols, &r[&1]) end)

            if Enum.all?(crows, fn r -> MapSet.member?(have, Enum.map(ccols, &r[&1])) end),
              do: [],
              else: [{:orphan, child, ccols, parent}]
        end
      end)

    dup ++ orphan
  end

  defp check(tables) do
    planted_dup = Map.update!(tables, "span", fn [r | rest] -> [r, r | rest] end)
    [row | rest] = tables["decider_input"]
    orphan = %{row | "asset_uuid" => "00000000-0000-7000-8000-000000000000"}
    planted_orphan = Map.put(tables, "decider_input", [orphan | rest])

    cond do
      problems(planted_dup) == [] ->
        {:error, :planted_duplicate_not_caught}

      problems(planted_orphan) == [] ->
        {:error, :planted_orphan_not_caught}

      (p = problems(tables)) != [] ->
        {:error, {:key_problems, p}}

      true ->
        IO.puts(
          "keys: #{map_size(tables)} tables, every primary key unique, every foreign key resolves; both planted faults caught"
        )

        :ok
    end
  end

  # --- denormalized -----------------------------------------------------------------------

  defp denormalize(t) do
    by = fn table, key -> Map.new(t[table] || [], &{&1[key], &1}) end
    span = by.("span", "span_uuid")
    detail = by.("span_detail", "span_uuid")
    asset = by.("asset", "asset_uuid")
    extent = by.("asset_extent", "asset_uuid")
    q = by.("question", "question")
    session = by.("session", "session_uuid")
    input = Map.new(t["decider_input"], &{{&1["span_uuid"], &1["input_role"]}, &1["asset_uuid"]})

    gold =
      Map.new(t["decider_gold"] || [], &{{&1["span_uuid"], &1["question"]}, &1["option_index"]})

    measure = Map.new(t["decider_measure"] || [], &{{&1["span_uuid"], &1["metric"]}, &1["value"]})

    options =
      t["question_option"]
      |> Enum.group_by(& &1["question"])
      |> Map.new(fn {k, v} ->
        {k, v |> Enum.sort_by(& &1["option_index"]) |> Enum.map(& &1["label"])}
      end)

    t["decider_question"]
    |> Enum.filter(
      &(input[{&1["span_uuid"], "view_before"}] && input[{&1["span_uuid"], "view_after"}])
    )
    |> Enum.sort_by(& &1["span_uuid"])
    |> Enum.map(fn dq ->
      id = dq["span_uuid"]
      sp = span[id]
      before = input[{id, "view_before"}]
      after_ = input[{id, "view_after"}]
      g = gold[{id, dq["question"]}]
      args = detail[id] && detail[id]["detail"]

      %{
        "example_uuid" => id,
        "concept" => @concept,
        "session_uuid" => sp["session_uuid"],
        "session_started_at" => session[sp["session_uuid"]]["started_at"],
        "started" => sp["started"],
        "tool" => sp["tool"],
        "state" => Enum.join(Enum.reject([sp["tool"], args], &is_nil/1), " "),
        "question" => dq["question"],
        "question_text" => q[dq["question"]]["text"],
        "options" => options[dq["question"]],
        "gold" => g && Enum.at(options[dq["question"]], g),
        "view_before_jxl" => asset[before]["bytes"],
        "view_before_width" => extent[before]["width"],
        "view_before_height" => extent[before]["height"],
        "view_after_jxl" => asset[after_]["bytes"],
        "view_after_width" => extent[after_]["width"],
        "view_after_height" => extent[after_]["height"],
        "gray_l1_8x8" => measure[{id, "gray_l1_8x8"}]
      }
    end)
  end

  # --- write ------------------------------------------------------------------------------

  defp write_all(tables, stage) do
    Enum.reduce_while(tables, :ok, fn {t, rows}, :ok ->
      case write(rows, Path.join(stage, "etnf/#{t}.parquet")) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp write(rows, path) do
    cols = rows |> hd() |> Map.keys() |> Enum.sort(&order/2)

    frame =
      cols
      |> Map.new(fn c -> {c, series(Enum.map(rows, & &1[c]))} end)
      |> DF.new()
      |> DF.select(cols)

    case DF.to_parquet(frame, path, compression: {:zstd, 9}) do
      :ok ->
        IO.puts("#{path}: #{length(rows)} rows")
        :ok

      {:error, reason} ->
        {:error, {:parquet, path, reason}}
    end
  end

  # Keys first, then the rest in name order.
  defp order(a, b) do
    ka = key?(a)
    kb = key?(b)
    if ka == kb, do: a <= b, else: ka
  end

  defp key?(c), do: String.ends_with?(c, "_uuid") or c in ~w(question concept)

  # Blobs are tagged {:blob, bytes}; the tag is read with elem/2, not a pattern (see the workspace
  # Elixir notes: a tuple pattern of throwaways is skipped).
  defp series(values) do
    case Enum.find(values, &(not is_nil(&1))) do
      first when :erlang.is_tuple(first) ->
        Series.from_list(Enum.map(values, &(&1 && elem(&1, 1))), dtype: :binary)

      _ ->
        Series.from_list(values)
    end
  end
end

case BuildSample.main(System.argv()) do
  :ok ->
    :ok

  {:error, :usage} ->
    IO.puts(:stderr, "usage: elixir build_sample.exs <store> <stage dir> <seed> <driver commit>")
    System.halt(2)

  {:error, reason} ->
    IO.puts(:stderr, "build failed: #{inspect(reason)}")
    System.halt(1)
end
