defmodule Broadway.Topology.Terminator do
  @moduledoc false
  use GenServer

  @spec start_link(term, GenServer.options()) :: GenServer.on_start()
  def start_link(args, opts) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  @spec trap_exit(GenServer.server()) :: :ok
  def trap_exit(terminator) do
    GenServer.call(terminator, :trap_exit)
  rescue
    _ -> :ok
  catch
    # If it is already down, we ignore it
    :exit, _ -> :ok
  end

  @spec drain(GenServer.server()) :: :ok
  def drain(terminator) do
    GenServer.call(terminator, :drain, :infinity)
  end

  @impl true
  def init(args) do
    state = %{
      producers: args[:producers],
      first: args[:first],
      last: args[:last]
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:trap_exit, _from, state) do
    Process.flag(:trap_exit, true)
    {:reply, :ok, state}
  end

  def handle_call(:drain, _from, state) do
    do_drain(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(_, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_, state) do
    do_drain(state)
    :ok
  end

  defp do_drain(state) do
    for name <- state.first, pid = safe_whereis(name) do
      send(pid, :will_terminate)
    end

    for name <- state.producers, pid = safe_whereis(name) do
      Broadway.Topology.ProducerStage.drain(pid)
    end

    for name <- state.last, pid = safe_whereis(name) do
      ref = Process.monitor(pid)

      receive do
        {:done, ^pid} -> :ok
        {:DOWN, ^ref, _, _, _} -> :ok
      end
    end
  end

  defp safe_whereis(name) do
    GenServer.whereis(name)
  rescue
    _ -> nil
  end
end
