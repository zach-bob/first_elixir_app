# https://elixir-lang.org/getting-started/mix-otp/genserver.html

defmodule KV.Registry do
  use GenServer

  ## Client API

  @doc """
  Starts the registry.
  """
  def start_link(opts) do
    # 1. Pass the name to GenServer's init
    server = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, server, opts)
  end

  @doc """
  Looks up the bucket pid for `name` stored in `server`.

  Returns `{:ok, pid}` if the bucket exists, `:error` otherwise.
  """
  def lookup(server, name) do
    # 2. Lookup is now done directly in ETS, without accessing the server
    case :ets.lookup(server, name) do
      [{^name, pid}] -> {:ok, pid}
      [] -> :error
    end
  end

  @doc """
  Ensures there is a bucket associated with the given `name` in `server`.
  """
  def create(server, name) do
    GenServer.call(server, {:create, name})
  end

  ## Defining GenServer Callbacks

  # informs the compiler that our intention for the subsequent function definition is to define a callback.
  @impl true
  def init(table) do
    # 3. We have replaced the names map by the ETS table
    names = :ets.new(table, [:named_table, read_concurrency: true])
    refs = %{}
    {:ok, {names, refs}}
  end

  # No longer necessary because we are using ETS to cache upon creation
  # No need for calling to the server anymore
  #
  # # Calls are synchronous and get back a reply
  # @impl true
  # def handle_call({:lookup, name}, _from, state) do
  #   {names, _} = state
  #   {:reply, Map.fetch(names, name), state}
  # end

  # Casts are async and do not receive a reply
  @impl true
  def handle_call({:create, name}, _from, {names, refs}) do
    # 5. Read and write to the ETS table instead of the map
    case lookup(names, name) do
      {:ok, pid} ->
        {:reply, pid, {names, refs}}

      :error ->
        # Use ETS table instead of mapping
        # if Map.has_key?(names, name) do
        #   {:noreply, {names, refs}}
        # else
        {:ok, pid} = DynamicSupervisor.start_child(KV.BucketSupervisor, KV.Bucket)
        ref = Process.monitor(pid)
        refs = Map.put(refs, ref, name)
        :ets.insert(names, {name, pid})
        # names = Map.put(names, name, pid)
        {:reply, pid, {names, refs}}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, {names, refs}) do
    {name, refs} = Map.pop(refs, ref)
    # 6. Delete from the ETS table instead of the map
    :ets.delete(names, name)
    # names = Map.delete(names, name)
    {:noreply, {names, refs}}
  end

  @impl true
  def handle_info(msg, state) do
    require Logger
    Logger.debug("Unexpected message in KV.Registry: #{inspect(msg)}")
    {:noreply, state}
  end
end
