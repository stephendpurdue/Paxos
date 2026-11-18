defmodule PaxosTest do

    # The functions implement 
    # the module specific testing logic
    defp init(name, participants, all \\ false) do
        cpid = TestHarness.wait_to_register(:coord, :global.whereis_name(:coord))
        pid = Paxos.start(name, participants, self())        
        TestHarness.wait_for(MapSet.new(participants), name, 
                        (if not all, do: length(participants)/2, 
                        else: length(participants)))
        {cpid, pid}
    end

    defp kill_paxos(pid, name) do
        Process.exit(pid, :kill)
        :global.unregister_name(name)
    end


    defp retry(_, _, 0), do: {:none, {:none, 0}}
    defp retry(pid, timeout, attempts) do
        receive do
            {:decide, val} -> {:decide, {val, attempts}}
            after timeout -> 
                if pid != :none, do: Paxos.start_ballot(pid)
                retry(pid, timeout, attempts - 1)
        end
    end

    # Test cases start from here

    # 1. No failures, no concurrent ballots
    def run_simple(name, participants, val) do
        {cpid, pid} = init(name, participants)
        send(cpid, :ready)
        {status, val, a} = receive do
            :start -> 
                IO.puts("#{inspect name}: started")
                Paxos.propose(pid, val)
                if name == (fn [h | _] -> h end).(participants), do: Paxos.start_ballot(pid)
                lid = if name == hd(Enum.reverse(participants)), do: pid, else: :none
                {status, {val, a}} = retry(lid, 1000, 10)
                if status != :none, do: IO.puts("#{name}: decided #{inspect val} after #{10 - a + 1} attempts"), 
                        else: IO.puts("#{name}: No decision after 10 seconds")
                {status, val, a}
        end
        send(cpid, :done)
        receive do
            :all_done -> 
                IO.puts("#{name}: #{inspect (ql = Process.info(pid, :message_queue_len))}")
                kill_paxos(pid, name)
                send cpid, {:finished, name, pid, status, val, a, ql}
        end
    end

    # 2. No failures, 2 concurrent ballots
    def run_simple_2(name, participants, val) do
        {cpid, pid} = init(name, participants)
        send(cpid, :ready)
        {status, val, a} = receive do
            :start -> 
                IO.puts("#{inspect name}: started")
                Paxos.propose(pid, val)
                if name in (fn [h1, h2 | _] -> [h1, h2] end).(participants), do: Paxos.start_ballot(pid)
                lid = if name == hd(Enum.reverse(participants)), do: pid, else: :none
                {status, {val, a}} = retry(lid, 1000, 10)
                if status != :none, do: IO.puts("#{name}: decided #{inspect val} after #{10 - a + 1} attempts"), 
                        else: IO.puts("#{name}: No decision after 10 seconds")
                {status, val, a}
        end
        send(cpid, :done)
        receive do
            :all_done -> 
                IO.puts("#{name}: #{inspect (ql = Process.info(pid, :message_queue_len))}")
                kill_paxos(pid, name)
                send cpid, {:finished, name, pid, status, val, a, ql}
        end
    end

    # 3. No failures, many concurrent ballots
    def run_simple_many(name, participants, val) do
        {cpid, pid} = init(name, participants)
        send(cpid, :ready)
        {status, val, a} = receive do
            :start -> 
                IO.puts("#{inspect name}: started")
                Paxos.propose(pid, val)
                for _ <- 1..10 do
                    Process.sleep(Enum.random(1..10))
                    Paxos.start_ballot(pid)
                end
                {status, {val, a}} = retry(pid, 1000, 10)
                if status != :none, do: IO.puts("#{name}: decided #{inspect val} after #{10 - a + 1} attempts"), 
                        else: IO.puts("#{name}: No decision after 10 seconds")
                {status, val, a}
        end
        send(cpid, :done)
        receive do
            :all_done -> 
                IO.puts("#{name}: #{inspect (ql = Process.info(pid, :message_queue_len))}")
                kill_paxos(pid, name)
                send cpid, {:finished, name, pid, status, val, a, ql}
        end
    end

    # 4. One non-leader process crashes, no concurrent ballots
    def run_non_leader_crash(name, participants, val) do
        {cpid, pid} = init(name, participants, true)
        send(cpid, :ready)
        {status, val, a, spare} = receive do
            :start -> 
                IO.puts("#{inspect name}: started")
                Paxos.propose(pid, val)
                if name == (leader = (fn [h | _] -> h end).(participants)), 
                        do: Paxos.start_ballot(pid)
                
                if name == (kill_p = hd(List.delete(participants, leader))) do
                    Process.sleep(Enum.random(1..5))
                    Process.exit(pid, :kill)
                    # IO.puts("KILLED #{kill_p}")
                end

                spare = List.delete(participants, kill_p)

                if name in  spare do
                    lid = if name == hd(Enum.reverse(spare)), do: pid, else: :none
                    {status, {val, a}} = retry(lid, 1000, 10)
                    if status != :none, do: IO.puts("#{name}: decided #{inspect val} after #{10 - a + 1} attempts"), 
                        else: IO.puts("#{name}: No decision after 10 seconds")
                    {status, val, a, spare}
                else
                    {:killed, :none, -1, spare}
                end
        end
        send(cpid, :done)
        receive do
            :all_done -> 
                ql = if name in spare do
                    IO.puts("#{name}: #{inspect (ql = Process.info(pid, :message_queue_len))}")
                    ql
                else 
                    {:message_queue_len, -1}
                end
                kill_paxos(pid, name)
                send cpid, {:finished, name, pid, status, val, a, ql}
        end
    end

    # You may want to add your own additional tests here....

end
