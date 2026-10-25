# Checks a decider-example sample (RFD66606-2334) against the trace store it came from, reading
# only the written files and the store:
#   - every column chunk of every Parquet file is zstd (read from each file's own footer)
#   - every *_uuid key is a UUIDv7 (RFC 9562), and each matches the store's id for that row
#   - the denormalized table's views are byte for byte the store's, and its gold counts match
# A planted one-byte change to a view and a planted non-v7 key must both be caught.
#
#   elixir check_sample.exs <stage dir> <trace-store.sqlite>

Mix.install([{:explorer, "~> 0.11"}, {:exqlite, "~> 0.27"}])

defmodule Thrift do
  @moduledoc "Just enough of Thrift's compact protocol to read a Parquet footer into maps."

  def struct_(bin), do: fields(bin, 0, %{})

  defp fields(<<0, rest::binary>>, _last, acc), do: {acc, rest}

  defp fields(<<delta::4, type::4, rest::binary>>, last, acc) do
    {id, rest} =
      if delta == 0 do
        {z, r} = varint(rest)
        {unzig(z), r}
      else
        {last + delta, rest}
      end

    {value, rest} = value(type, rest)
    fields(rest, id, Map.put(acc, id, value))
  end

  defp value(1, rest), do: {true, rest}
  defp value(2, rest), do: {false, rest}
  defp value(3, <<b::signed-8, rest::binary>>), do: {b, rest}

  defp value(t, rest) when t in [4, 5, 6] do
    {z, rest} = varint(rest)
    {unzig(z), rest}
  end

  defp value(7, <<d::little-float-64, rest::binary>>), do: {d, rest}

  defp value(8, rest) do
    {n, rest} = varint(rest)
    <<bytes::binary-size(^n), rest::binary>> = rest
    {bytes, rest}
  end

  defp value(t, <<size::4, etype::4, rest::binary>>) when t in [9, 10] do
    {n, rest} = if size == 15, do: varint(rest), else: {size, rest}
    elements(etype, n, rest, [])
  end

  defp value(11, rest) do
    case varint(rest) do
      {0, rest} ->
        {%{}, rest}

      {n, <<kt::4, vt::4, rest::binary>>} ->
        {pairs, rest} =
          Enum.reduce(1..n, {[], rest}, fn _, {acc, r} ->
            {k, r} = value(kt, r)
            {v, r} = value(vt, r)
            {[{k, v} | acc], r}
          end)

        {Map.new(pairs), rest}
    end
  end

  defp value(12, rest), do: fields(rest, 0, %{})

  # In a list a bool is one byte: 1 true, 2 false.
  defp elements(_t, 0, rest, acc), do: {Enum.reverse(acc), rest}

  defp elements(t, n, <<b, rest::binary>>, acc) when t in [1, 2],
    do: elements(t, n - 1, rest, [b == 1 | acc])

  defp elements(t, n, rest, acc) do
    {v, rest} = value(t, rest)
    elements(t, n - 1, rest, [v | acc])
  end

  defp varint(bin, shift \\ 0, acc \\ 0)

  defp varint(<<1::1, b::7, rest::binary>>, shift, acc),
    do: varint(rest, shift + 7, acc + (b <<< shift))

  defp varint(<<0::1, b::7, rest::binary>>, shift, acc), do: {acc + (b <<< shift), rest}

  defp unzig(z), do: Bitwise.bxor(z >>> 1, -Bitwise.band(z, 1))

  defp a <<< b, do: Bitwise.bsl(a, b)
  defp a >>> b, do: Bitwise.bsr(a, b)
end

defmodule CheckSample do
  alias Explorer.DataFrame, as: DF

  @zstd 6

  # FileMetaData.row_groups (4) -> RowGroup.columns (1) -> ColumnChunk.meta_data (3) -> codec (4).
  def codecs(path) do
    with {:ok, bin} <- File.read(path),
         size = byte_size(bin),
         <<len::little-32, "PAR1">> <- binary_part(bin, size - 8, 8) do
      {meta, _} = Thrift.struct_(binary_part(bin, size - 8 - len, len))

      {:ok,
       for rg <- Map.get(meta, 4, []), chunk <- Map.get(rg, 1, []), into: MapSet.new() do
         chunk |> Map.get(3, %{}) |> Map.get(4)
       end}
    else
      {:error, _} = e -> e
      _ -> {:error, {:not_parquet, path}}
    end
  end

  def v7?(text) do
    case Regex.run(
           ~r/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
           text || ""
         ) do
      nil -> false
      _ -> true
    end
  end

  def main([stage, store]) do
    files = Path.wildcard(Path.join(stage, "**/*.parquet"))

    with {:ok, conn} <- Exqlite.Sqlite3.open(store, mode: :readonly),
         {:ok, ids} <- store_ids(conn),
         {:ok, views} <- store_views(conn),
         {:ok, gold} <- store_gold(conn),
         {:ok, tables} <- read_tables(files),
         [r | _] = den <- tables["decider_denormalized"] || {:error, :no_denormalized_rows} do
      not_zstd =
        Enum.reject(files, fn f ->
          case codecs(f) do
            {:ok, set} -> set == MapSet.new([@zstd])
            _ -> false
          end
        end)

      keys =
        for {_, rows} <- tables, row <- rows, {k, v} <- row, String.ends_with?(k, "_uuid"), do: v

      not_v7 = Enum.reject(keys, &v7?/1)
      unknown = Enum.reject(keys, &MapSet.member?(ids, &1))

      differ =
        Enum.count(den, fn r ->
          views[{r["example_uuid"], "view_before"}] != r["view_before_jxl"] or
            views[{r["example_uuid"], "view_after"}] != r["view_after_jxl"]
        end)

      den_gold = den |> Enum.map(& &1["gold"]) |> Enum.reject(&is_nil/1) |> Enum.frequencies()

      # Controls: the checks above see one changed byte and one non-v7 key.
      flipped =
        r["view_before_jxl"]
        |> :binary.bin_to_list()
        |> List.update_at(-1, &Bitwise.bxor(&1, 1))
        |> :binary.list_to_bin()

      control_view = views[{r["example_uuid"], "view_before"}] != flipped

      probe =
        Path.join(
          System.tmp_dir!(),
          "check_sample_snappy_#{System.unique_integer([:positive])}.parquet"
        )

      :ok = DF.to_parquet(DF.new(x: [1, 2, 3]), probe, compression: :snappy)
      control_zstd = codecs(probe) != {:ok, MapSet.new([@zstd])}
      File.rm(probe)
      control_v7 = not v7?(r["example_uuid"] |> String.replace(~r/^(.{14})7/, "\\g{1}4"))

      IO.puts("#{length(files)} parquet files; not zstd: #{length(not_zstd)}")

      IO.puts(
        "#{length(keys)} uuid keys; not UUIDv7: #{length(not_v7)}; not the store's: #{length(unknown)}"
      )

      IO.puts("#{length(den)} denormalized rows; views differing from the store: #{differ}")
      IO.puts("gold store #{inspect(gold)} sample #{inspect(den_gold)}")

      IO.puts(
        "controls fire: changed byte #{control_view}, non-v7 key #{control_v7}, snappy file #{control_zstd}"
      )

      failures =
        [
          not_zstd != [],
          not_v7 != [],
          unknown != [],
          differ != 0,
          gold != den_gold,
          not control_view,
          not control_zstd,
          not control_v7
        ]

      if Enum.any?(failures), do: {:error, :check_failed}, else: IO.puts("OK")
    end
  end

  def main(_), do: {:error, :usage}

  defp read_tables(files) do
    Enum.reduce_while(files, {:ok, %{}}, fn f, {:ok, acc} ->
      case DF.from_parquet(f) do
        {:ok, df} -> {:cont, {:ok, Map.put(acc, Path.rootname(Path.basename(f)), DF.to_rows(df))}}
        {:error, reason} -> {:halt, {:error, {:unreadable, f, reason}}}
      end
    end)
  end

  defp store_ids(conn) do
    with {:ok, rows} <-
           rows(
             conn,
             "SELECT uuid FROM session UNION ALL SELECT uuid FROM span UNION ALL SELECT uuid FROM asset"
           ) do
      {:ok, MapSet.new(rows, &hd/1)}
    end
  end

  defp store_views(conn) do
    sql = """
    SELECT s.uuid, r.name, a.bytes FROM decider_input i
    JOIN input_role r USING (input_role_id) JOIN asset a ON a.asset_id = i.asset_id
    JOIN span s ON s.span_id = i.span_id
    """

    with {:ok, rows} <- rows(conn, sql),
         do: {:ok, Map.new(rows, fn [u, role, bytes] -> {{u, role}, bytes} end)}
  end

  defp store_gold(conn) do
    with {:ok, rows} <-
           rows(
             conn,
             "SELECT gold, count(*) FROM decider_row WHERE gold IS NOT NULL GROUP BY gold"
           ) do
      {:ok, Map.new(rows, fn [g, n] -> {g, n} end)}
    end
  end

  defp rows(conn, sql) do
    with {:ok, st} <- Exqlite.Sqlite3.prepare(conn, sql),
         {:ok, rows} <- Exqlite.Sqlite3.fetch_all(conn, st),
         :ok <- Exqlite.Sqlite3.release(conn, st),
         do: {:ok, rows}
  end
end

case CheckSample.main(System.argv()) do
  :ok ->
    :ok

  {:error, :usage} ->
    IO.puts(:stderr, "usage: elixir check_sample.exs <stage dir> <trace-store.sqlite>")
    System.halt(2)

  {:error, reason} ->
    IO.puts(:stderr, "check failed: #{inspect(reason)}")
    System.halt(1)
end
