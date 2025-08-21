defmodule KVStoreTest do
  use ExUnit.Case
  doctest KVStore

  test "greets the world" do
    assert KVStore.hello() == :world
  end
end
